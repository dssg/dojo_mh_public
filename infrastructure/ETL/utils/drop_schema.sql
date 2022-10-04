/* This file creates a schema using the write role and grants permission for
   the read role to read from the tables. You must use three variables when
   running it:

    - write: the PostgreSQL role to use for creating objects in the database
    - schema: the name of the schema to create
    - read: the role which should be given read privileges (only) to tables in the schema
*/
set role :write;
drop schema if exists :schema cascade;

