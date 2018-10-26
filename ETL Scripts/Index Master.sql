

use pacifica;


/*
Strategy

Should not apply new indexes on production tables without review.   Since they could impact production performce.

But for large tables like FrameState it seems clear I will need an index on Modified  datetime.



*/


create index ndx_fs_modified on framestate (modified) ; 

create index ndx_f_modified on frame (modified) ; 



