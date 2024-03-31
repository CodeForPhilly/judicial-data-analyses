# workspace ----
setwd(dirname(.rs.api.getSourceEditorContext()$path))
library(tidyverse)
library(glue)

# get URLs ----
statute_home <- "https://www.legis.state.pa.us"

statute_listings <- 
  file.path(statute_home, "cfdocs/legis/LI/Public/cons_index.cfm") |> 
  read_lines()

statute_urls <- 
  tibble(
    site =
      statute_listings |> 
      keep(~str_detect(.x, "txtType=HTM")) |> 
      str_extract('https[^"]*'),
    title = str_extract(site, "\\d+$"),
    iframe_url = # the site ^^ uses an iframe that has this pattern vv
      glue("{statute_home}//WU01/LI/LI/CT/HTM/{title}/{title}.HTM")
  ) |> 
  filter(as.integer(title) > 0)


scrape_html <- function(title_number, overwrite = FALSE) {
  # title_number <- "78"
  
  print(title_number)
  
  csv_name <- glue("output/title-{title_number}.csv")
  
  # early exit
  if (file.exists(csv_name) && !overwrite) {
    return(invisible())
  }
  
  iframe_html <- 
    statute_urls$iframe_url[statute_urls$title == title_number] |> 
    read_lines()
  
  # some titles are just an editorial note or odd like title 59
  if (
    !any(str_detect(iframe_html, "TITLE"))
    & !any(str_detect(iframe_html, "Chapter"))
  ) {
    return(invisible())
  }
  
  html_df <- 
    tibble(text = iframe_html) |> 
    filter(
      #str_detect(text, "none&#xA"),
      cumsum(str_detect(text, ">TITLE \\d+<")) == 1
    ) |> 
    mutate(
      text =
        str_remove(text, '.*">') |> 
        str_remove("(?<=[[:alnum:]])<.*") |> 
        str_remove_all("&nbsp;")
    ) |> 
    filter(
      str_detect(text, "[[:alnum:]]"),
      !str_detect(text, "\\s*</p>$")
    )
  
  res <- 
    html_df |>
    mutate(
      title_number = 
        str_extract(text, "(?<=TITLE )\\d+") |> 
        str_pad(width = 2, pad = "0"),
      title_text = 
        ifelse(!is.na(title_number), lead(text), NA) |> 
        str_to_title(),
      
      chapter_number = str_extract(text, "(?<=Chapter )\\d+(?=\\.)"),
      chapter_text = 
        ifelse(!is.na(chapter_number), text, NA) |> 
        str_extract("(?<=\\d\\.).*") |> 
        str_remove("<.*"),
      
      offense_section = str_extract(text, "(?<=&#167; )[\\d\\.]+(?=\\.)"),
      offense_text = 
        ifelse(!is.na(offense_section), text, NA) |> 
        str_extract("(?<=\\d\\.).*") |> 
        str_remove_all(".*>")
    ) |> 
    fill(title_number:chapter_text) |> 
    drop_na(offense_section) |> 
    select(-text)
  
  write_csv(x = res, file = csv_name)
}

# loop ----
'-------------------------------------------------------------
may need to terminate R & run multiple times as loop hangs??
adding gc() or Sys.sleep(1) did not help :(

to stop R from command line:
  if only 1 session:
    taskkill /im rsession-utf8.exe /f

  otherwise find PID (tasklist) and end it (taskkill): 
    tasklist /v | find "rsession"
    taskkill /pid xxxx /f
--------------------------------------------------------------'

statute_urls |> 
  pull(title) |> 
  walk(
    .f = ~scrape_html(.x),
    .progress = list(type = c("iterator"), clear = FALSE)
  )

# combine ----
all_statutes <- 
  read_csv(
    file = list.files("output", full.names = TRUE),
    col_types = cols(.default = "c")
  )

prep_statutes <- 
  all_statutes |> 
  #
  mutate(
    section_number = str_extract(offense_section, "^\\d+"),
    section_text = ifelse(str_detect(offense_section, "\\."), NA, offense_text),
    subsection_number = str_extract(offense_section, "(?<=\\d\\.)\\d+"),
    subsection_text = ifelse(!is.na(subsection_number), offense_text, NA)
  ) |> 
  fill(section_text) |> 
  mutate(
    across(where(is.character), ~trimws(.x) |> str_remove("\\.$")),
    across(c(chapter_number, section_number), ~as.integer(.x))
  ) |> 
  arrange(
    title_number, 
    chapter_number, 
    as.integer(section_number),
    replace_na(subsection_number, "0") |> as.integer()
  ) 

prep_statutes |> filter(str_detect(offense_section, "^11018")) |> print(n = 20)

write_csv(prep_statutes, "statute_hierarchy.csv")
