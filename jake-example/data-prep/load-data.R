# ---- about code ---------------------------------------------------------------
"This code loads the raw data for the 2024 Socical Justice Hackathon 
into a .duckdb file

* The code ensures all table and column names use snake_case and lowercase

* Writes a .duckdb file at a location of your choice

* This script is only for loading data, another method will be used for 
  removing columns that have PII or not deemed helpful

Justification for odd choices:
* The data as pipe-delimited-.txt files posed a challenge, the first step is
  to change them all to .csv
  
* The function duckdb::duckdb_read_csv() was not robust enough for the 
  arguments needed to read in the files. A CTAS (create table as) 
  statement was used instead because:
  
    * Errors arose with max row size exceeding 2,097,152 bytes, used
      > read_csv(..., ignore_errors = true)
      
    * A alpha character in zip code fields caused errors also. 
      Reading in numeric values as characters, will need to edit after
      > read_csv(..., auto_type_candidates = ['null', 'date', 'time', 'varchar'])
        

Caveats: 
* If using both a SQL IDE (ODBC) and R/Py/cli, make sure that the versions match 
  or the the one with read-only access is >= version able to write
  https://duckdb.org/docs/installation/index?version=stable
  
Next Steps:
* tables that have a field like 'sentence_conditions' will need to be de-duped
  for example: 
    select * 
    from cpcms_sentence_data 
    where otn = 'L8332262'
"

# ---- setup -----
library(tidyverse)
library(glue)
library(duckdb)

db_name <- "input/jat-hackathon.duckdb"

con <-
  dbConnect(
    drv = duckdb(), 
    dbdir = db_name
  )


# ---- convert csv to txt -----
local({
  txt_paths <- list.files(pattern = "txt$", full.names = TRUE)

  # check if there are txt files, ensures you you don't run this twice
  if (length(txt_paths)) {
    csv_paths <- str_replace(txt_paths, "txt$", "csv")
      
    file.rename(from = txt_paths, to = csv_paths)
  }
})


# ---- duckdb -----
file_paths <- list.files(pattern = "csv$", full.names = TRUE) 

file_table_names <-
  basename(file_paths) |>
  tools::file_path_sans_ext() |>
  janitor::make_clean_names()

#file <- file_paths[1]
#' @examples
#' add_and_rename_cols(file = file_paths[7], con, overwrite = TRUE)
add_and_rename_cols <- function(file, con, overwrite = TRUE) {
  
  i <- which(file_paths == file)
  table_name = file_table_names[i]
  
  # progress
  print(
    glue(
      n = length(file_paths),
      "{i} / {n} : {basename(file)} -> {table_name}"
    )
  )

  already_exists <- dbExistsTable(con, table_name)

  if (already_exists) {
    if (overwrite) {
      dbRemoveTable(con, table_name)
    } else {
      return(invisible())
    }
  }

  ctas <- # [C]reate [T]able [as]
    glue(
      "create table {table_name} as 
      select *
      from 
        read_csv(
          '{file}',
          auto_type_candidates = ['null', 'date', 'time', 'varchar'],
          delim = '|',
          header = true,
          ignore_errors = true,
          --rejects_table = 'rejects_table',
          sample_size = 1000
        )"
    )
  
  # else read into db
  dbSendStatement(con, statement = ctas)

  # rename fields
  old_names <- dbListFields(con, table_name)

  col_rename_statments <-
    glue(
      new = janitor::make_clean_names(old_names),
      "alter table {table_name} rename column {old_names} to {new};"
    ) |> 
    glue_collapse("\n")

  # write to db
  dbExecute(con, col_rename_statments)
}


# ---- write -----
walk(
  .x = file_paths,
  .f = 
    possibly(
      ~add_and_rename_cols(
        file = .x, 
        con = con,
        overwrite = TRUE
      )
    ),
  .progress = TRUE
)

# update_cols ----
info_col_types <- 
  dbGetQuery(
    con, 
    "select 
      table_name,
      column_name,
      ordinal_position,
      data_type
    from 
      information_schema.columns"
  ) |> 
  as_tibble()

likely_numeric <- 
  regex(
    "
    am(oun)?t
    |balance
    |credit
    |days|hours|months|years
    |max
    |(?<!(court|docket|link)_)number
    ",
    comments = TRUE
  )

cols_to_update <- 
  info_col_types |> 
  filter(data_type == "VARCHAR") |> 
  #arrange(column_name) |> 
  filter(
    str_detect(column_name, likely_numeric)
  )

cols_to_update |> 
  glue_data(
    "alter table {table_name} alter {column_name} type double;"
  ) |> 
  glue_collapse("\n") |> 
  dbSendStatement(con = con)


# sample queries ----

dbListFields(con, "cpcms_payment_plan_data")


dbGetQuery(# number of records
  con, 
  "select count_star() from cpcms_payment_plan_data"
)


dbListTables(con)


dbGetQuery(# distinct # of values
  con, 
  "select 
      payment_plan_frequency,
      count_star() as n,
      approx_count_distinct(otn) as n_otn
  from 
      cpcms_payment_plan_data
  group by 
      payment_plan_frequency
  order by 
      n desc"
)


dbGetQuery(# look at an example
  con,
  "select * from cpcms_sentence_data where otn = 'L8332262'"
) |> 
  as_tibble() |> 
  gather() |> 
  print(n = Inf)

# all columns ----
info_cols <- dbGetQuery(con, "select * from information_schema.columns")

info_cols |> 
  select(table_name, column_name, data_type) |> 
  as_tibble()

# ---- disconnect ----
duckdb::dbDisconnect(con, shutdown = TRUE)








dbListFields(con, table_name)
dbListTables(con)







file_cols_list <-
  map(
    file_paths,
    ~{
      df <- read_n_lines(.x, 100)
      tibble(
        col_name = names(df),
        class = sapply(df, class) |> map_chr(pluck, 1)
      )
    }
  )

file_cols <- 
  list_rbind(
    file_cols_list,
    names_to = "file"
  )


column_overlap <- 
  file_cols |> 
  group_by(col_name) |> 
  summarise(
    n_table = n(),
    n_class = n_distinct(class),
    classes = paste(sort(unique(class)), collapse = ", ")
  )
    



read_and_assign <- function(file_name, n = 50) {
  obj_name <- 
    basename(file_name) |> 
    tools::file_path_sans_ext() |> 
    janitor::make_clean_names()
  
  if (exists(obj_name, envir = globalenv()) && !overwrite) {
    print("already exists")
    return(invisible())
  }
  
  df <- 
    read_delim(file_name, delim = "|", n_max = n) |> 
    as_tibble() |> 
    janitor::clean_names()

  assign(
    x = obj_name, 
    value = df, 
    envir = env
  )
}



walk(
  file_paths, 
  ~read_and_assign(.x)
)


duckdb_read_csv(
  conn = con,
  name = table_name,
  files = file,
  nrow.check = Inf,
  delim = "|"
)
