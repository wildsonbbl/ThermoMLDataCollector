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
    type == "Mole fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Mole fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

## analysis

tmlframe %>% nrow()
tmlframe %>%
  summary()

### checking phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  view()


tmlframe %>%
  group_by(m1_phase, m2_phase) %>%
  summarise(n = n()) %>%
  arrange(desc(n))

tmlframe <- tmlframe %>%
  filter(
    phase_1 %in% c("Liquid", "Liquid mixture 1"),
    phase_2 %in% c("Liquid", "Liquid mixture 2", "Liquid mixture 1")
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))


### Fill in missing mole fraction info

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c2p1 = 1 - `Mole fraction c1 phase_1`,
    mole_fraction_c2p2 = 1 - `Mole fraction c1 phase_2`,
    mole_fraction_c1 = if_else(
      is.na(m1),
      1 - m2,
      m1,
    ),
    mole_fraction_c2 = if_else(
      is.na(m2),
      1 - m1,
      m2
    )
  )

tmlframe %>%
  filter(`Mole fraction c1 phase_1` > 0 | `Mole fraction c1 phase_2` > 0) %>%
  view()

tmlframe %>%
  summary()

### filter columns

tmlframe <- tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking molecules available

tmlframe %>%
  distinct(inchi1, inchi2) %>%
  nrow()

tmlframe %>%
  filter(
    (
      grepl("ammonium", c1, ignore.case = TRUE) |
        grepl("ammonium", c2, ignore.case = TRUE)
    )
  ) %>%
  view()

tmlframe %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE)
    )
  ) %>%
  view()

## Save

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "lle_binary.parquet"
  )

tml_saved <- read_parquet("lle_binary.parquet")
tml_saved %>% colnames()
