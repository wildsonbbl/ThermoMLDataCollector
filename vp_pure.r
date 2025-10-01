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

## analysis

tmlframe %>% nrow()
tmlframe %>%
  summary()

### checking and filtering phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3, m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))


tmlframe %>%
  group_by(m0_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  filter(m0_phase == "Liquid") %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking molecules available
tmlframe %>%
  filter(
    `Temperature, K` > 600
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
