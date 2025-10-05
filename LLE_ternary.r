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
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlset %>%
  filter(
    type == "Mole fraction",
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

tmlframe %>% colnames()

### checking phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  view()


tmlframe %>%
  filter(phase_1 == "Liquid", phase_2 == "Liquid") %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()

tmlframe %>%
  filter(
    grepl("Liquid", phase_1, ignore.case = TRUE),
    grepl("Liquid", phase_2, ignore.case = TRUE),
    is.na(phase_3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlframe <- tmlframe %>%
  filter(
    grepl("Liquid", phase_1, ignore.case = TRUE),
    grepl("Liquid", phase_2, ignore.case = TRUE),
    is.na(phase_3)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))


### Fill in missing mole fraction info

tmlframe %>%
  filter(m1_phase_1 > 1 | m1_phase_2 > 1) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()

tmlframe %>%
  filter(
    !is.na(m1_phase_1),
    is.na(m2_phase_1),
    is.na(m3_phase_1),
    is.na(`Solvent for m1_phase_1`),
    is.na(`Mole fraction c3 phase_1`),
    is.na(`Mole fraction c2 phase_1`)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()


tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p1 = if_else(
      is.na(m1_phase_1) & !is.na(m2_phase_1) & !is.na(m3_phase_1),
      1 - m2_phase_1 - m3_phase_1,
      m1_phase_1,
    ),
    mole_fraction_c2p1 = if_else(
      !is.na(m1_phase_1) & is.na(m2_phase_1) & !is.na(m3_phase_1),
      1 - m1_phase_1 - m3_phase_1,
      m2_phase_1,
    ),
    mole_fraction_c3p1 = if_else(
      !is.na(m1_phase_1) & !is.na(m2_phase_1) & is.na(m3_phase_1),
      1 - m1_phase_1 - m2_phase_1,
      m3_phase_1,
    ),
    mole_fraction_c1p2 = if_else(
      is.na(m1_phase_2) & !is.na(m2_phase_2) & !is.na(m3_phase_2),
      1 - m2_phase_2 - m3_phase_2,
      m1_phase_2,
    ),
    mole_fraction_c2p2 = if_else(
      !is.na(m1_phase_2) & is.na(m2_phase_2) & !is.na(m3_phase_2),
      1 - m1_phase_2 - m3_phase_2,
      m2_phase_2,
    ),
    mole_fraction_c3p2 = if_else(
      !is.na(m1_phase_2) & !is.na(m2_phase_2) & is.na(m3_phase_2),
      1 - m1_phase_2 - m2_phase_2,
      m3_phase_2,
    ),
  ) %>%
  mutate(
    mole_fraction_c1p1 = if_else(
      !is.na(`Mole fraction c1 phase_1`) & is.na(mole_fraction_c1p1),
      `Mole fraction c1 phase_1`,
      mole_fraction_c1p1
    ),
    mole_fraction_c1p2 = if_else(
      !is.na(`Mole fraction c1 phase_2`) & is.na(mole_fraction_c1p2),
      `Mole fraction c1 phase_2`,
      mole_fraction_c1p2
    ),
    mole_fraction_c2p1 = if_else(
      !is.na(`Mole fraction c2 phase_1`) & is.na(mole_fraction_c2p1),
      `Mole fraction c2 phase_1`,
      mole_fraction_c2p1
    ),
    mole_fraction_c2p2 = if_else(
      !is.na(`Mole fraction c2 phase_2`) & is.na(mole_fraction_c2p2),
      `Mole fraction c2 phase_2`,
      mole_fraction_c2p2
    ),
    mole_fraction_c3p1 = if_else(
      !is.na(`Mole fraction c3 phase_1`) & is.na(mole_fraction_c3p1),
      `Mole fraction c3 phase_1`,
      mole_fraction_c3p1
    ),
    mole_fraction_c3p2 = if_else(
      !is.na(`Mole fraction c3 phase_2`) & is.na(mole_fraction_c3p2),
      `Mole fraction c3 phase_2`,
      mole_fraction_c3p2
    ),
  ) %>%
  mutate(
    mole_fraction_c1p1 = if_else(
      is.na(mole_fraction_c1p1),
      1 - mole_fraction_c2p1 - mole_fraction_c3p1,
      mole_fraction_c1p1
    ),
    mole_fraction_c2p1 = if_else(
      is.na(mole_fraction_c2p1),
      1 - mole_fraction_c1p1 - mole_fraction_c3p1,
      mole_fraction_c2p1
    ),
    mole_fraction_c3p1 = if_else(
      is.na(mole_fraction_c3p1),
      1 - mole_fraction_c1p1 - mole_fraction_c2p1,
      mole_fraction_c3p1
    ),
    mole_fraction_c1p2 = if_else(
      is.na(mole_fraction_c1p2),
      1 - mole_fraction_c2p2 - mole_fraction_c3p2,
      mole_fraction_c1p2
    ),
    mole_fraction_c2p2 = if_else(
      is.na(mole_fraction_c2p2),
      1 - mole_fraction_c1p2 - mole_fraction_c3p2,
      mole_fraction_c2p2
    ),
    mole_fraction_c3p2 = if_else(
      is.na(mole_fraction_c3p2),
      1 - mole_fraction_c1p2 - mole_fraction_c2p2,
      mole_fraction_c3p2
    ),
  )

tmlframe %>%
  summary()

tmlframe %>%
  filter(
    mole_fraction_c3p2 < 0
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()

tmlframe %>% colnames()

tmlframe %>%
  filter(
    !is.na(`Solvent: Mole fraction c1 phase_1`) |
      !is.na(`Solvent: Mole fraction c1 phase_2`) |
      !is.na(`Solvent: Mole fraction c2 phase_1`) |
      !is.na(`Solvent: Mole fraction c3 phase_1`)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  view()

### merge temperature and pressure

tmlframe %>%
  mutate(
    T_K = if_else(
      is.na(`Temperature, K phase_1`),
      `Temperature, K phase_2`,
      `Temperature, K phase_1`
    ),
    P_kPa = if_else(
      is.na(`Pressure, kPa phase_1`),
      `Pressure, kPa phase_2`,
      `Pressure, kPa phase_1`
    )
  ) %>%
  mutate(T_K = if_else(
    is.na(T_K), 298.15, T_K
  )) %>%
  summary()

tmlframe <- tmlframe %>%
  mutate(
    T_K = if_else(
      is.na(`Temperature, K phase_1`),
      `Temperature, K phase_2`,
      `Temperature, K phase_1`
    ),
    P_kPa = if_else(
      is.na(`Pressure, kPa phase_1`),
      `Pressure, kPa phase_2`,
      `Pressure, kPa phase_1`
    )
  ) %>%
  mutate(T_K = if_else(
    is.na(T_K), 298.15, T_K
  ))


### merge mole fractions

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1p1),
    !is.na(mole_fraction_c2p1),
    !is.na(mole_fraction_c3p1)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tml_p1 <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1p1),
    !is.na(mole_fraction_c2p1),
    !is.na(mole_fraction_c3p1)
  ) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p1,
    mole_fraction_c2 = mole_fraction_c2p1,
    mole_fraction_c3 = mole_fraction_c3p1
  )

tmlframe %>%
  filter(
    !is.na(mole_fraction_c1p2),
    !is.na(mole_fraction_c2p2),
    !is.na(mole_fraction_c3p2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tml_p2 <- tmlframe %>%
  filter(
    !is.na(mole_fraction_c1p2),
    !is.na(mole_fraction_c2p2),
    !is.na(mole_fraction_c3p2)
  ) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p2,
    mole_fraction_c2 = mole_fraction_c2p2,
    mole_fraction_c3 = mole_fraction_c3p2
  )

tml_combined <- bind_rows(tml_p1, tml_p2) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

tml_combined %>% summary()

### checking molecules available

tml_combined %>%
  distinct(inchi1, inchi2) %>%
  nrow()

tml_combined %>%
  filter(
    (
      grepl("ammonium", c1, ignore.case = TRUE) |
        grepl("ammonium", c2, ignore.case = TRUE)
    )
  ) %>%
  view()

tml_combined %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE)
    )
  ) %>%
  view()

## Save

tml_combined %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "lle_ternary.parquet"
  )

tml_saved <- read_parquet("lle_ternary.parquet")
tml_saved %>% colnames()
