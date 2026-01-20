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

tmlset %>%
  filter(
    type == "Mass fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  summary()

tmlset %>%
  filter(
    type == "Mass fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n))


tmlframe <- tmlset %>%
  filter(
    type == "Mole fraction",
    is.na(c3)
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### checking phases

tmlframe %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  summary()

tmlframe %>%
  filter(
    phase_1 == "Liquid",
    grepl("Liquid", phase_2, ignore.case = TRUE)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe %>%
  filter(
    grepl("Liquid", phase_1, ignore.case = TRUE),
    grepl("Liquid", phase_2, ignore.case = TRUE)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()


tmlframe <- tmlframe %>%
  filter(
    grepl("Liquid", phase_1, ignore.case = TRUE),
    grepl("Liquid", phase_2, ignore.case = TRUE)
  ) %>%
  select(where(~ !all(is.na(.x))))

### Fill in missing mole fraction info

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p1 = if_else(
      is.na(m1_phase_1),
      1 - m2_phase_1,
      m1_phase_1
    ),
    mole_fraction_c1p2 = if_else(
      is.na(m1_phase_2),
      1 - m2_phase_2,
      m1_phase_2
    ),
    mole_fraction_c2p1 = if_else(
      is.na(m2_phase_1),
      1 - m1_phase_1,
      m2_phase_1,
    ),
    mole_fraction_c2p2 = if_else(
      is.na(m2_phase_2),
      1 - m1_phase_2,
      m2_phase_2
    )
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
  ) %>%
  mutate(
    mole_fraction_c1p1 = if_else(
      is.na(mole_fraction_c1p1) & !is.na(mole_fraction_c2p1),
      1 - mole_fraction_c2p1,
      mole_fraction_c1p1
    ),
    mole_fraction_c1p2 = if_else(
      is.na(mole_fraction_c1p2) & !is.na(mole_fraction_c2p2),
      1 - mole_fraction_c2p2,
      mole_fraction_c1p2
    ),
    mole_fraction_c2p1 = if_else(
      !is.na(mole_fraction_c1p1) & is.na(mole_fraction_c2p1),
      1 - mole_fraction_c1p1,
      mole_fraction_c2p1
    ),
    mole_fraction_c2p2 = if_else(
      !is.na(mole_fraction_c1p2) & is.na(mole_fraction_c2p2),
      1 - mole_fraction_c1p2,
      mole_fraction_c2p2
    ),
  )


tmlframe %>%
  filter(
    !is.na(mole_fraction_c1p1),
    !is.na(mole_fraction_c1p2)
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  summary()

tmlframe %>%
  filter(
    is.na(`Temperature, K phase_1`),
    is.na(`Temperature, K phase_2`)
  ) %>%
  summary()

tmlframe %>%
  summary()

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

tml_p1 <- tmlframe %>%
  filter(!is.na(mole_fraction_c1p1), !is.na(mole_fraction_c2p1)) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p1,
    mole_fraction_c2 = mole_fraction_c2p1
  )

tml_p2 <- tmlframe %>%
  filter(!is.na(mole_fraction_c1p2), !is.na(mole_fraction_c2p2)) %>%
  rename(
    mole_fraction_c1 = mole_fraction_c1p2,
    mole_fraction_c2 = mole_fraction_c2p2
  )

tml_combined <- bind_rows(tml_p1, tml_p2)

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
  summary()

tml_combined %>%
  filter(
    (
      grepl("choline", c1, ignore.case = TRUE) |
        grepl("choline", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_combined %>%
  filter(
    (
      grepl("amine", c1, ignore.case = TRUE) |
        grepl("amine", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

tml_combined %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE)
    )
  ) %>%
  summary()

## Save

tml_combined %>%
  select(where(~ !all(is.na(.x)))) %>%
  write_parquet(
    .,
    "lle_binary.parquet"
  )

tml_saved <- read_parquet("lle_binary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()
