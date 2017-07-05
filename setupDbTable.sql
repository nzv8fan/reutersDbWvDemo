CREATE TABLE reuters_sgm (
   row_index_pk serial PRIMARY KEY,
   category text default NULL,
   title text NOT NULL,
   body text NOT NULL
 );
