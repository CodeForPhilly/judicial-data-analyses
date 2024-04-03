setwd(dirname(.rs.api.getSourceEditorContext()$path))
library(tidyverse)

read_csv_char <- partial(read_csv, col_types = cols(.default = "c"))

df <- 
  bind_rows(
    read_csv_char("statute_webscrape.csv"),
    read_csv_char("statute_manual.csv")
  ) |>
  mutate(
    title_number = as.integer(title_number),
    across(ends_with("text"), ~ str_to_sentence(.x)),
    section_text =
      ifelse(
        is.na(article_number),
        section_text,
        glue("Article {article_number} - {article_text}: {section_text}")
      )
  ) |>
  select(-starts_with("article"))

arrow::write_parquet(df, "statute_hierarchy.parquet")
