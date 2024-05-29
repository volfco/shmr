create table inode
(
    id        integer not null
        primary key autoincrement,
    size      integer not null,
    /* access time */
    atime     integer,
    /* modified time */
    mtime     integer not null,
    /* change time */
    ctime     integer not null,
    /* creation time */
    crtime    integer not null,
    kind      text    not null,
    perm      integer not null,
    nlink     integer not null,
    uid       integer not null,
    gid       integer not null,
    rdev      integer not null,
    blksize   integer not null,
    flags     integer not null,
    has_xattr INT
);

create table directory
(
    parent integer
        constraint directory_inode_id_fk
            references inode,
    entry  blob,
    inode  integer
        constraint directory_inode_id_fk_2
            references inode,
    constraint directory_pk
        primary key (parent, entry)
);

create table inode_xattrs
(
    inode integer,
    key   blob,
    value blob
);

create index inode_xattrs_inode_index
    on inode_xattrs (inode);

create table storage_block
(
    inode       integer,
    idx         integer,
    size        integer,
    topology    TEXT
);

create table storage_block_shard
(
    inode     integer,
    idx       integer,
    shard_idx integer not null,
    pool      text    not null,
    bucket    text    not null,
    filename  text    not null,
    constraint storage_block_shard_storage_block_inode_idx_fk
        foreign key (inode, idx) references storage_block (inode, idx)
);

