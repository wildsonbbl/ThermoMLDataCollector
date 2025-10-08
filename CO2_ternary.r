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

tmlframe <- tmlset %>%
  filter(
    type == "Mole fraction",
    !is.na(c3),
    (
      inchi1 == "InChI=1S/CO2/c2-1-3" |
        inchi2 == "InChI=1S/CO2/c2-1-3" |
        inchi3 == "InChI=1S/CO2/c2-1-3"
    )
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

tmlframe %>% colnames()

### checking phases

tmlframe <- tmlframe %>%
  filter(
    phase_1 == "Gas",
    phase_2 == "Liquid"
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

### Fill in missing mole fraction info

tmlframe %>% colnames()


tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p1 = case_when(
      !is.na(m1_phase_1) ~ m1_phase_1,
      is.na(m1_phase_1) & !is.na(m2_phase_1) & !is.na(m3_phase_1) ~ 1 - m2_phase_1 - m3_phase_1,
      !is.na(`Mole fraction c1 phase_1`) & is.na(m1_phase_1) ~ `Mole fraction c1 phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ 1.0,
      !is.na(`Pressure, kPa c2 phase_1`) ~ 0.0,
      inchi1 == "InChI=1S/CO2/c2-1-3" ~ 1.0,
      TRUE ~ 0.0
    ),
    mole_fraction_c2p1 = case_when(
      !is.na(m2_phase_1) ~ m2_phase_1,
      !is.na(m1_phase_1) & is.na(m2_phase_1) & !is.na(m3_phase_1) ~ 1 - m1_phase_1 - m3_phase_1,
      !is.na(`Mole fraction c2 phase_1`) & is.na(m2_phase_1) ~ `Mole fraction c2 phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ 0.0,
      !is.na(`Pressure, kPa c2 phase_1`) ~ 1.0,
      inchi2 == "InChI=1S/CO2/c2-1-3" ~ 1.0,
      TRUE ~ 0.0
    ),
    mole_fraction_c3p1 = case_when(
      !is.na(m3_phase_1) ~ m3_phase_1,
      !is.na(m1_phase_1) & !is.na(m2_phase_1) & is.na(m3_phase_1) ~ 1 - m1_phase_1 - m2_phase_1,
      !is.na(`Mole fraction c3 phase_1`) & is.na(m3_phase_1) ~ `Mole fraction c3 phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ 0.0,
      !is.na(`Pressure, kPa c2 phase_1`) ~ 0.0,
      inchi3 == "InChI=1S/CO2/c2-1-3" ~ 1.0,
      TRUE ~ 0.0
    ),
    mole_fraction_c1p2 = case_when(
      !is.na(m1_phase_2) ~ m1_phase_2,
      is.na(m1_phase_2) & !is.na(m2_phase_2) & !is.na(m3_phase_2) ~ 1 - m2_phase_2 - m3_phase_2,
      !is.na(`Mole fraction c1 phase_2`) & is.na(m1_phase_2) ~ `Mole fraction c1 phase_2`,
      !is.na(`Solvent: Mole fraction c1 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * `Solvent: Mole fraction c1 phase_2`,
      !is.na(`Solvent: Mole fraction c1 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * `Solvent: Mole fraction c1 phase_2`,
      !is.na(`Solvent: Mass fraction c1 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * (
        `Solvent: Mass fraction c1 phase_2` / molweight1 / (
          `Solvent: Mass fraction c1 phase_2` / molweight1 + (1 - `Solvent: Mass fraction c1 phase_2`) / molweight3
        )
      ),
      !is.na(`Solvent: Mass fraction c1 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * (
        `Solvent: Mass fraction c1 phase_2` / molweight1 / (
          `Solvent: Mass fraction c1 phase_2` / molweight1 + (1 - `Solvent: Mass fraction c1 phase_2`) / molweight2
        )
      ),
      !is.na(`Solvent: Molality, mol/kg c1 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * (
        `Solvent: Molality, mol/kg c1 phase_2` / (
          `Solvent: Molality, mol/kg c1 phase_2` + 1000 / molweight3
        )
      ),
      !is.na(`Solvent: Molality, mol/kg c1 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * (
        `Solvent: Molality, mol/kg c1 phase_2` / (
          `Solvent: Molality, mol/kg c1 phase_2` + 1000 / molweight2
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * (
        `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` / (
          `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` + 1
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * (
        `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` / (
          `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` + 1
        )
      ),
      TRUE ~ NA
    ),
    mole_fraction_c2p2 = case_when(
      !is.na(m2_phase_2) ~ m2_phase_2,
      !is.na(m1_phase_2) & is.na(m2_phase_2) & !is.na(m3_phase_2) ~ 1 - m1_phase_2 - m3_phase_2,
      !is.na(`Mole fraction c2 phase_2`) & is.na(m2_phase_2) ~ `Mole fraction c2 phase_2`,
      !is.na(`Solvent: Mole fraction c2 phase_2`) & !is.na(m1_phase_2) ~ (1 - m1_phase_2) * `Solvent: Mole fraction c2 phase_2`,
      !is.na(`Solvent: Mole fraction c2 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * `Solvent: Mole fraction c2 phase_2`,
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(m1_phase_2) ~ (1 - m1_phase_2) * (
        `Solvent: Mass fraction c2 phase_2` / molweight2 / (
          `Solvent: Mass fraction c2 phase_2` / molweight2 + (1 - `Solvent: Mass fraction c2 phase_2`) / molweight3
        )
      ),
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * (
        `Solvent: Mass fraction c2 phase_2` / molweight2 / (
          `Solvent: Mass fraction c2 phase_2` / molweight2 + (1 - `Solvent: Mass fraction c2 phase_2`) / molweight1
        )
      ),
      !is.na(`Solvent: Molality, mol/kg c2 phase_2`) & !is.na(m1_phase_2) ~ (1 - m1_phase_2) * (
        `Solvent: Molality, mol/kg c2 phase_2` / (
          `Solvent: Molality, mol/kg c2 phase_2` + 1000 / molweight3
        )
      ),
      !is.na(`Solvent: Molality, mol/kg c2 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * (
        `Solvent: Molality, mol/kg c2 phase_2` / (
          `Solvent: Molality, mol/kg c2 phase_2` + 1000 / molweight1
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) & !is.na(m1_phase_2) ~ (1 - m1_phase_2) * (
        `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` / (
          `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` + 1
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) & !is.na(m3_phase_2) ~ (1 - m3_phase_2) * (
        `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` / (
          `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` + 1
        )
      ),
      TRUE ~ NA
    ),
    mole_fraction_c3p2 = case_when(
      !is.na(m3_phase_2) ~ m3_phase_2,
      !is.na(m1_phase_2) & !is.na(m2_phase_2) & is.na(m3_phase_2) ~ 1 - m1_phase_2 - m2_phase_2,
      !is.na(`Mole fraction c3 phase_2`) & is.na(m3_phase_2) ~ `Mole fraction c3 phase_2`,
      !is.na(`Amount ratio of solute to solvent c3 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * `Amount ratio of solute to solvent c3 phase_2` / (
        `Amount ratio of solute to solvent c3 phase_2` + 1
      ),
      !is.na(`Solvent: Mole fraction c3 phase_2`) & !is.na(m1_phase_2) ~ (1 - m1_phase_2) * `Solvent: Mole fraction c3 phase_2`,
      !is.na(`Solvent: Mole fraction c3 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * `Solvent: Mole fraction c3 phase_2`,
      !is.na(`Solvent: Mass fraction c3 phase_2`) & !is.na(m1_phase_2) ~ (1 - m1_phase_2) * (
        `Solvent: Mass fraction c3 phase_2` / molweight3 / (
          `Solvent: Mass fraction c3 phase_2` / molweight3 + (1 - `Solvent: Mass fraction c3 phase_2`) / molweight2
        )
      ),
      !is.na(`Solvent: Mass fraction c3 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * (
        `Solvent: Mass fraction c3 phase_2` / molweight3 / (
          `Solvent: Mass fraction c3 phase_2` / molweight3 + (1 - `Solvent: Mass fraction c3 phase_2`) / molweight1
        )
      ),
      !is.na(`Mass fraction c3 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * (
        `Mass fraction c3 phase_2` / molweight3 / (
          `Mass fraction c3 phase_2` / molweight3 + (1 - `Mass fraction c3 phase_2`) / molweight1
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) & !is.na(m2_phase_2) ~ (1 - m2_phase_2) * (
        `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2` / (
          `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2` + 1
        )
      ),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) & !is.na(m1_phase_2) ~ (1 - m1_phase_2) * (
        `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2` / (
          `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2` + 1
        )
      ),
      TRUE ~ NA
    )
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
  ) %>%
  filter(
    mole_fraction_c1p2 < 1.001,
    mole_fraction_c2p2 < 1.001,
  )



### merge temperature and pressure

tmlframe <- tmlframe %>%
  mutate(
    T_K = if_else(
      is.na(`Temperature, K phase_1`),
      `Temperature, K phase_2`,
      `Temperature, K phase_1`
    ),
    P_kPa = case_when(
      !is.na(`Pressure, kPa phase_2`) ~ `Pressure, kPa phase_2`,
      !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`,
      !is.na(`Pressure, kPa c1 phase_1`) ~ `Pressure, kPa c1 phase_1`,
      !is.na(`Pressure, kPa c2 phase_1`) ~ `Pressure, kPa c2 phase_1`,
      TRUE ~ NA
    )
  ) %>%
  filter(
    !is.na(P_kPa),
  )

### checking molecules available

tmlframe %>%
  filter(
    (
      grepl("ammonium", c1, ignore.case = TRUE) |
        grepl("ammonium", c2, ignore.case = TRUE) |
        grepl("ammonium", c3, ignore.case = TRUE)
    )
  ) %>%
  view()

tmlframe %>%
  filter(
    c1 == "carbon dioxide",
    c2 == "ethanol",
    c3 == "water",
    T_K == 298
  ) %>%
  select(T_K, P_kPa, mole_fraction_c1p2, mole_fraction_c2p2, mole_fraction_c3p2) %>%
  view()

tmlframe %>%
  filter(
    (
      grepl("amin", c1, ignore.case = TRUE) |
        grepl("amin", c2, ignore.case = TRUE) |
        grepl("amin", c3, ignore.case = TRUE)
    )
  ) %>%
  view()

tmlframe %>%
  filter(
    (
      grepl("choline", c1, ignore.case = TRUE) |
        grepl("choline", c2, ignore.case = TRUE) |
        grepl("choline", c3, ignore.case = TRUE)
    )
  ) %>%
  view()

tmlframe %>%
  filter(
    (
      grepl("imidazolium", c1, ignore.case = TRUE) |
        grepl("imidazolium", c2, ignore.case = TRUE) |
        grepl("imidazolium", c3, ignore.case = TRUE)
    )
  ) %>%
  view()

## Save

tmlframe %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.)))) %>%
  write_parquet(
    .,
    "co2_ternary.parquet"
  )

tml_saved <- read_parquet("co2_ternary.parquet")
tml_saved %>% colnames()
