<?php

// LRUFileCache
// Least-Recently-Used file cache
//
// by Tom Gidden <gid@starberry.tv>
// Copyright (C) 2013, Starberry Ltd


/*
$cache = new LRUFileCache('tmp', 3, 1024*1024*1024);
$cache->sync();
...
$hash = $cache->hash('some identifying information, eg. filename, URL');
$cache->add($file, $hash, LRUFileCache::COPY);
...
$hash = $cache->hash('some identifying information, eg. filename, URL');
$filename = $cache->get($hash);
*/


class LRUFileCache
{
    public function __construct($cache_dir, $cache_depth=3, $max_cache_size=1073741824 /* 1GB */)
    // Initialise the file cache:
    // -  $cache_dir:      the (existing) directory to store the hash tree in
    // -  $cache_depth:    how many levels of directory to use in the hash tree
    // -  $max_cache_size: maximum size of the cache directory tree, in bytes
    {
        if(!is_dir($cache_dir)) {
            throw new Exception("Bad cache directory '$cache_dir'");
        }

        $this->cache_dir = $cache_dir;
        $this->cache_depth = $cache_depth;
        $this->max_cache_size = $max_cache_size / 512; // Size is in 512-byte blocks
    }


    // Methods for adding to the cache
    const COPY = 0;             // Safest, default method
    const MOVE = 1;             // Useful when the original file is a temp file
    const LINK = 2;             // Neat, but be wary of changing original file

    public function hash($str)
    // Hashing function to convert nice pretty identifiers -- what you
    // humans call "filenames" -- into nice neat hashes, so we can have a
    // balanced cache hierarchy. All of this code operates on hashes,
    // rather than filenames.
    //
    // Since crypto can be slow, we're hashing. However, come to think of
    // it, as it stands, this is a memory leak.
    //
    // TODO: fix memory leak. Maybe expire old hashes using an LRU. Or sod
    // it: computers are fast these days.
    //
    {
        if(isset($this->hashcache[$str])) {
            return $this->hashcache[$str];
        }

        // Security isn't a requirement for the hashing function, and MD5
        // still seems to be the fastest algo with a unique enough result
        // in most circumstances.

        return ($this->hashcache[$str] = md5($str));
    }

    public function cachePath($hash)
    // Take a hash and convert it into an actual filesystem path for the
    // cached file.
    {
        // This regexp will match hex-encoded MD5s, and hopefully any others
        // you might choose to configure instead.
        if(!preg_match('/^[0-9a-f]{32,}$/', $hash)) {
            throw new Exception("Bad hash for cachePath: ".$hash);
        }

        // Build the directory tree branch for the file
        $dir = $this->cache_dir;

        for($i=$this->cache_depth; $i>0; $i--) {
            $dir .= '/'.substr($hash, $i, 1);
        }

        // Make the directory structure if it doesn't already exist.
        if(!is_dir($dir)) {
            mkdir($dir, 0775, true);
        }

        // And slap the whole hash on the end as the leaf name.
        return $dir . '/' . $hash;
    }

    public function sync()
    // Drill down through the file cache and build the in-memory cache
    // indices.
    {
        // Start off with empty in-memory cache indices:
        $this->clear();

        // And now recursively traverse the cache hierarchy.

        // I tried doing this asynchronously, by counting directory
        // entries and exits, and only proceeding with the next step when
        // that count returned to zero, but it kept getting stuck. So I
        // gave up and used synchronous fs calls: this is not usually a
        // problem, as sync() is only intended to run when the server
        // starts, so it doesn't matter how slow it is... right?
        $toAdd = array();

        // Okay, start at the top of the tree.
        $this->sync_catalogueDir($toAdd, $this->cache_dir);

        // Now that's done, we sort the files we've found by time: oldest first.
        usort($toAdd, function ($a, $b) {
                return $a->time - $b->time;
            });

        // And add them all in order to the cache.
        foreach ($toAdd as $item) {
            // Wire up the neighbours
            $item->prev = $this->newest_item;

            if($this->newest_item) {
                $this->newest_item->next = $item;
            }

            if(!$this->oldest_item) {
                $this->oldest_item = $item;
            }

            $this->newest_item = $item;

            // and add to the index.
            $this->items_by_hash[$item->hash] = $item;
        }
    }

    public function get($hash)
    // Get the file path of an item in the cache, marking it as used
    {
        // If the item isn't in the cache, then return FALSE.
        if(!isset($this->items_by_hash[$hash]))
            return FALSE;

        $item = $this->items_by_hash[$hash];

        // First, update the timestamp in the memory cache: it's an access
        // time, not just modified time.  The calling routine should at
        // least open the file to update the access time on the file
        // itself. This method should do it too, but it's a waste if the
        // caller's likely to do it anyway.
        $item->time = time();

        // Get the file path: we'll need it to return at the end, but we
        // also want to check for cache consistency
        $cache_path = $this->cachePath($hash);

        // Check it exists. It should. If not, this is exceptional.
        if(!file_exists($cache_path))
            throw new Exception("Inconsistent cache: $hash not found");
            #return FALSE;

        // If it's the newest item, we can just continue.
        if($item !== $this->newest_item)
            return $cache_path;

        // Else it's not the newest, so let's promote it by rewiring its
        // neighbours.

        // First, we wire the two neighbours together, removing
        // the item from the double-linked list.
        $oprev = $item->prev;
        $onext = $item->next;

        if($oprev) {
            $oprev->next = $onext;
        }

        if($onext) {
            $onext->prev = $oprev;
        }

        // Then we put it at the top of the list
        $item->prev = $this->newest_item;
        $item->next = null;

        // And set the old newest item to the next newest.
        if($this->newest_item) {
            $this->newest_item->next = $item;
        }

        // Finally, bung it at the top.
        $this->newest_item = $item;

        // And schedule a future (well, very near future)
        // asynchronous cache expiry check.
        $this->_checkCacheSize_async();

        // Return the file path in the cache.
        return $cache_path;
    }

    public function add($tmp_path, $hash=null, $method=self::COPY)
    // Copy (or link or move) the file passed from tmp_path to the cache,
    // and put it at the top of the in-memory cache list. Hash can be
    // optional, but better to use a good representation of the item to
    // derive the hash manually (eg. the source URL of the file)
    {
        // If the hash wasn't supplied, then we just use the input file
        // path. It's not ideal.
        if(empty($hash)) {
            $hash = $this->hash($tmp_path);
        }

        // Work out where to put it.
        $cache_path = $this->cachePath($hash);

        // We're first going to establish if the item is already stored in the cache.
        $exists = file_exists($cache_path);

        // So, does a version of the item already exist in the cache?
        if($exists) {
            // Yes, so before we move the item into the cache, we need to
            // remove the old one.
            unlink($cache_path);
        }

        // Now, get a stat on the existing temp item. We'll need this to
        // correctly insert or update the item into the cache list.
        $stat = stat($tmp_path);

        // Put the file into the cache, using whatever method has been specified
        switch ($method) {
        case self::LINK:
            // Link the item into its new position in the cache. This is
            // efficient, but it means any changes to the source file will
            // get included in the cache. Sounds nice, but somewhat borks
            // the cache's size estimate thing.
            link($tmp_path, $cache_path);
            break;

        case self::MOVE:
            // Move the item into its new position in the cache.
            rename($tmp_path, $cache_path);
            break;

        case self::COPY:
        default:
            // Copy the file. Storage- and performance inefficient, but this is the
            // "safest" method, as it has minimum side-effects.
            copy($tmp_path, $cache_path);
        }

        // Now the file stuff is done, we can add it to the cache list

        if(isset($this->items_by_hash[$hash])) {
            // The item's already in the LRU, so we need to make sure
            // it's the newest.

            $item = $this->items_by_hash[$hash];

            // First, update the timestamp: it's an access time, not
            // just modified time.
            $item->time = time();

            if($item !== $this->newest_item) {
                // It's not the newest, so let's promote it by rewiring
                // its neighbours.

                // First, we wire the two neighbours together, removing
                // the item from the double-linked list.
                $oprev = $item->prev;
                $onext = $item->next;

                if($oprev) {
                    $oprev->next = $onext;
                }

                if($onext) {
                    $onext->prev = $oprev;
                }

                // Then we put it at the top of the list
                $item->prev = $this->newest_item;
                $item->next = null;

                // And set the old newest item to the next newest.
                if($this->newest_item) {
                    $this->newest_item->next = $item;
                }

                // Finally, bung it at the top.
                $this->newest_item = $item;

                // And schedule a future (well, very near future)
                // asynchronous cache expiry check.
                $this->_checkCacheSize_async();
            }

            // All done, so now we can return the new file location
            return $cache_path;
        }

        else { // ie. !item

            // The item is not in the cache already, so we need to put
            // it there... at the very top.

            // Create the item
            $item = new LRUFileCacheItem($hash, time(), $stat['blocks']);

            // Put the current newest item as the new item's next
            // newest.
            $item->prev = $this->newest_item;

            // If there was actually a newest_item there, then wire
            // it to the new newest.
            if($this->newest_item) {
                $this->newest_item->next = $item;
            }

            // And now keep track of this new head-of-the-list.
            $this->newest_item = $item;

            // But if there was no oldest item (ie. this was an empty list)
            // then the new newest is also the oldest.
            if(!$this->oldest_item) {
                $this->oldest_item = $item;
            }

            // And index the item for finding it later.
            $this->items_by_hash[$hash] = $item;

            // Update the running size total.
            $this->total_size += $item->size;

            // And schedule a future (well, very near future)
            // asynchronous cache expiry check.
            $this->_checkCacheSize_async();

            // All done, so now we can return the new file location
            return $cache_path;
        }
    }

    public function remove($hash)
    // Remove the item from the cache
    {
        if(!isset($this->items_by_hash[$hash])) {
            // It's not there, so don't worry about it. Not even worth an error?
            return;
        }

        // Get it so we can rewire
        $item = $this->items_by_hash[$hash];

        // Remove it from the index
        unset($this->items_by_hash[$hash]);

        // Rewire the neighbours: smush them to each other
        $oprev = $item->prev;
        $onext = $item->next;

        if($oprev) {
            $oprev->next = $onext;
        }

        if($onext) {
            $onext->prev = $oprev;
        }

        if($item===$this->oldest_item) {
            $this->oldest_item = $onext;
        }

        if($item===$this->newest_item) {
            $this->newest_item = $oprev;
        }

        // And finally, remove the file itself.
        $cache_path = $this->cachePath($hash);
        unlink($cache_path);
    }

    ////////////////////////////////////////////////////////////////////////////

    // items_by_hash: an index of all items in the immediate cache list, by
    // their hash identifier.
    private $items_by_hash = array();

    // We also double-link all items in the cache with previous/next and
    // oldest, newest. This gives us the direct accessibility of the hash
    // index above; the sequential order necessary for the
    // Least-Recently-Used caching; plus the ability to rewire the
    // sequential list without too many weird array ops. We could do this
    // with pop(), shift(), delete() and some splicing, but it would end
    // up with a very messy array.  Instead, let's push the boat out and
    // store some links, even if they are strictly redundant.
    private $oldest_item = null;
    private $newest_item = null;

    // And let's cache the crypto while we're at it. No point in MD5'ing a
    // lot, and it lets us be sloppier about keeping track of things.
    private $hashcache = array();

    // We keep a running total of the size of the cache so we know when to
    // expire.
    private $total_size = 0;

    // The max size of the disk cache, measured in 512-byte
    // blocks. Important to note that this is a target size for the disk
    // cache, and will be exceeded momentarily.
    private $max_cache_size;


    private function sync_catalogueDir(&$toAdd, $path)
    // Recursive function to load the cache directory tree
    {
        // Open the cache directory at this level
        if(FALSE === ($dh = opendir($path))) {
            throw new Exception("Cannot open directory '$path'");
        }

        // For each member of the directory...
        while(FALSE !== ($fn = readdir($dh))) {

            // Skip dotfiles
            if('.'===$fn[0]) continue;

            $npath = $path . '/' . $fn;

            if(is_dir($npath)) {
                // If it's a directory, then we'll need to traverse it. Screw it... do it now.
                $this->sync_catalogueDir($toAdd, $npath);
            }
            else if(is_file($npath)) {
                // If it's a file, then add it to the todo list

                // Get the stats so we can find out size and time
                $stat = stat($npath);

                // Work out a sensible access/modified time to use.
                $time = $stat['mtime'];// $stat['atime']>$stat['mtime'] ? $stat['atime'] : $stat['mtime'];

                // Add it to the todo list
                $toAdd[] = new LRUFileCacheItem($fn, $time, $stat['blocks']);
            }

            else {
                // Else, someone's been putting crap in my cache! I'm ANGRY!
                throw new Exception("Bad file(s) in cache: ".$hash);
            }
        }
    }

    private function _checkCacheSize_async()
    // A remnant of this code's previous life as a Node.js module.
    {
        $this->checkCacheSize();
    }

    private function checkCacheSize()
    // This private method is used to check the size of the cache and
    // trigger expiries when needed.
    {
        fwrite(STDERR, "Checking cache... ".$this->total_size." / ".$this->max_cache_size."\n");

        // Loop through the cache, removing items from the oldest end until we
        // fit inside the cache limit.
        while ($this->oldest_item and $this->total_size >= $this->max_cache_size) {
            fwrite(STDERR, "Expiring ".$this->oldest_item->hash." ".count($this->items_by_hash)." ".$this->total_size." / ".$this->max_cache_size."\n");

            // We could probably do this quicker by acting directly on
            // oldest_item, rather than looking up through the index
            // again. However, I can't be bothered to dupe the file
            // removal code. This is neater.
            $this->remove($this->oldest_item->hash);
        }
    }

    private function clear()
    // Clear the memory cache. This doesn't actually remove items from the
    // cache: it's just for reinitialising the in-memory cacheq list of
    // the file cache.
    {
        // Clear the index, the easy way:
        $this->items_by_hash = array();

        // And then invalidate the list, item-by-item.
        $i = $this->oldest_item;
        while($i) {
            $oi = $i;
            $i = $i->next;
            $oi->prev = null;
        }
        $this->oldest_item = null;
        $this->newest_item = null;
        $this->total_size = 0;
    }
}


class LRUFileCacheItem
{
    // Each item in the cache keeps time (not really necessary), the size
    // (to tot up the size of the cache), the name (so we can refer to it
    // in the index), and links to neighbour items in the cache
    // list.
    public $hash, $time, $size, $prev, $next;

    public function __construct ($hash, $time, $size)
    {
        $this->hash = $hash;
        $this->time = $time;
        $this->size = $size;
        $this->prev = null;
        $this->next = null;
    }
}