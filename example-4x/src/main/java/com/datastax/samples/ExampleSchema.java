package com.datastax.samples;

/**
 * Externalization of schema constant.
 */
public interface ExampleSchema {
    
	String KEYSPACE_NAME                    = "killrvideo";
    int    KEYSPACE_REPLICATION_FACTOR      = 1;
    
    /**
     * Will be used for this table:
     * 
     * CREATE TABLE IF NOT EXISTS users (
     *  email      text,
     *  firstname  text,
     *  lastname   text,
     *  PRIMARY KEY (email)
     * );
     */
    String USER_TABLENAME = "users";
    String USER_EMAIL     = "email";
    String USER_FIRSTNAME = "firstname";
    String USER_LASTNAME  = "lastname";
    
    /**
     * CREATE TYPE IF NOT EXISTS video_format (
     *   width   int,
     *   height  int
     *);
     */
    String UDT_VIDEO_FORMAT_NAME   = "video_format";
    String UDT_VIDEO_FORMAT_WIDTH  = "width";
    String UDT_VIDEO_FORMAT_HEIGHT = "height";
    
    /**
     * CREATE TABLE IF NOT EXISTS videos (
     *   videoid    uuid,
     *   title      text,
     *   upload    timestamp,
     *   email 	    text,
     *   url        text,
     *   tags       set <text>,
     *   frames     list<int>,
     *   formats    map <text,frozen<video_format>>,
     *   PRIMARY KEY (videoid)
     * ); 
     **/
    String VIDEO_TABLENAME  = "videos";
    String VIDEO_VIDEOID    = "videoid";
    String VIDEO_TITLE      = "title";
    String VIDEO_UPLOAD     = "upload";
    String VIDEO_USER_EMAIL = "email";
    String VIDEO_FRAMES     = "frames";
    String VIDEO_URL        = "url";
    String VIDEO_TAGS       = "tags";
    String VIDEO_FORMAT     = "formats";
    
    /**
     * CREATE TABLE IF NOT EXISTS videos_views (
     *    videoid     uuid,
     *    views       counter,
     *    PRIMARY KEY (videoid)
     * );
     */
    String VIDEO_VIEWS_TABLENAME  = "videos_views";
    String VIDEO_VIEWS_VIDEOID    = "videoid";
    String VIDEO_VIEWS_VIEWS      = "views";
    
    /**
     * CREATE TABLE IF NOT EXISTS comments_by_video (
     *   videoid uuid,
     *   commentid timeuuid,
     *   userid uuid,
     *   comment text,
     *   PRIMARY KEY (videoid, commentid)
     * ) WITH CLUSTERING ORDER BY (commentid DESC);
     * 
     * 
     * CREATE TABLE IF NOT EXISTS comments_by_user (
     *   userid uuid,
     *   commentid timeuuid,
     *   videoid uuid,
     *   comment text,
     *   PRIMARY KEY (userid, commentid)
     * ) WITH CLUSTERING ORDER BY (commentid DESC);
     */
    String COMMENT_BY_VIDEO_TABLENAME  = "comments_by_video";
    String COMMENT_BY_VIDEO_VIDEOID    = "videoid";
    String COMMENT_BY_VIDEO_COMMENTID  = "commentid";
    String COMMENT_BY_VIDEO_USERID     = "userid";
    String COMMENT_BY_VIDEO_COMMENT    = "comment";
    String COMMENT_BY_USER_TABLENAME   = "comments_by_user";
    String COMMENT_BY_USER_VIDEOID     = COMMENT_BY_VIDEO_VIDEOID;
    String COMMENT_BY_USER_COMMENTID   = COMMENT_BY_VIDEO_COMMENTID;
    String COMMENT_BY_USER_USERID      = COMMENT_BY_VIDEO_USERID;
    String COMMENT_BY_USER_COMMENT     = COMMENT_BY_VIDEO_COMMENT;
    
    String FILES_TABLENAME             = "files";
    String FILES_FILENAME              = "filename";
    String FILES_EXTENSION             = "extension";
    String FILES_UPLOAD                = "upload";
    String FILES_BINARY                = "binary";
    
}
