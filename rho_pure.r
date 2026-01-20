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

tmlset %>%
  filter(is.na(inchi1), !is.na(c1)) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

## selecting properties of interest

tmlset %>%
  filter(
    type == "Mass density, kg/m3",
    is.na(c2)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Mass density, kg/m3",
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

tmlframe <- tmlframe %>%
  filter(
    phase_1 == "Liquid",
    is.na(phase_2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking molecules available

tmlframe %>%
  filter(
    `Temperature, K phase_1` > 600
  ) %>%
  summary()

## Save

tmlframe %>% summary()

tmlframe %>%
  rename(
    T_K = `Temperature, K phase_1`,
    rho = m0_phase_1,
    P_kPa = `Pressure, kPa phase_1`
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "rho_pure.parquet"
  )

tml_saved <- read_parquet("rho_pure.parquet")
tml_saved %>% colnames()


## DES

tmlframe %>%
  filter(
    grepl("ammonium", c1, ignore.case = TRUE)
  ) %>%
  group_by(c1, inchi1) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  summary()
