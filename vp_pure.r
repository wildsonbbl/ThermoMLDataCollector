library(tidyverse)
library(arrow)
library(data.table)

## loading file

files <- list.files("./data/raw",
  "*.parquet",
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
    type == "Vapor or sublimation pressure, kPa",
    is.na(c2)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Vapor or sublimation pressure, kPa",
    is.na(c2),
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe %>%
  filter(phase_1 == "Gas", phase_2 == "Liquid") %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(phase_1 == "Gas", phase_2 == "Liquid") %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking molecules available
tmlframe %>%
  filter(
    `Temperature, K phase_2` > 600
  ) %>%
  view()

## Save

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "vp_pure.parquet"
  )

tml_saved <- read_parquet("vp_pure.parquet")
tml_saved %>% colnames()
