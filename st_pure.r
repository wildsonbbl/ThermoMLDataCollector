library(tidyverse)
library(arrow)
library(data.table)

## loading file

files <- list.files("./data/raw",
  "dataset.parquet",
  full.names = TRUE
) %>% sort()
files

tmlset <- open_dataset(files, unify_schemas = TRUE)

tmlset %>%
  colnames() %>%
  sort()

## checking type of data

tmlset %>%
  select(type) %>%
  as.data.frame() %>%
  distinct() %>%
  arrange(type)

## selecting properties of interest

tmlset %>%
  filter(
    type == "Surface tension liquid-gas, N/m",
    is.na(c2)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Surface tension liquid-gas, N/m",
    is.na(c2)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

## Save

tmlframe %>% summary()

tmlframe %>%
  rename(
    T_K = `Temperature, K phase_2`,
    st = m0_phase_2,
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "st_pure.parquet"
  )

tml_saved <- read_parquet("st_pure.parquet")
tml_saved %>% colnames()


## DES

tmlframe %>%
  filter(
    grepl("ammonium", c1, ignore.case = TRUE)
  ) %>%
  group_by(c1, inchi1) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  view()
