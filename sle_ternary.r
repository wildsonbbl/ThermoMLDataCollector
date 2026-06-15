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
    type %in% c(
      "Solid-liquid equilibrium temperature, K"
    ),
    !is.na(c2),
    !is.na(c3)
  ) %>%
  as.data.frame() %>%
  group_by(phase_1, phase_2, phase_3) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  print(n = 9)

tmlframe <- tmlset %>%
  filter(
    type %in% c(
      "Solid-liquid equilibrium temperature, K"
    ),
    !is.na(c2),
    !is.na(c3),
  ) %>%
  as.data.frame() %>%
  select(where(~ !all(is.na(.x)))) %>%
  select(all_of(sort(names(.))))

## get temperature and pressure

tmlframe <- tmlframe %>%
  mutate(
    T_K = case_when(
      !is.na(m0_phase_1) ~ m0_phase_1,
      !is.na(m0_phase_2) ~ m0_phase_2,
    ),
    P_kPa = case_when(
      !is.na(`Pressure, kPa phase_2`) ~ `Pressure, kPa phase_2`,
      !is.na(`Pressure, kPa phase_1`) ~ `Pressure, kPa phase_1`
    )
  )

### get mass fraction

tmlframe %>%
  select(matches(c("c[1-3] phase_2"))) %>%
  colnames()

tmlframe <- tmlframe %>%
  mutate(
    mass_fraction_c1p2 = case_when(
      !is.na(`Mass fraction c1 phase_2`) ~ `Mass fraction c1 phase_2`,
      !is.na(`Solvent: Mass fraction c1 phase_2`) & !is.na(`Mass fraction c2 phase_2`) ~
        `Solvent: Mass fraction c1 phase_2` * (1 - `Mass fraction c2 phase_2`),
      !is.na(`Solvent: Mass fraction c1 phase_2`) & !is.na(`Mass fraction c3 phase_2`) ~
        `Solvent: Mass fraction c1 phase_2` * (1 - `Mass fraction c3 phase_2`),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~
        `Molality, mol/kg c1 phase_2` * molweight1 /
          (1000 + `Molality, mol/kg c1 phase_2` * molweight1 + `Molality, mol/kg c2 phase_2` * molweight2),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~
        `Molality, mol/kg c1 phase_2` * molweight1 /
          (1000 + `Molality, mol/kg c1 phase_2` * molweight1 + `Molality, mol/kg c3 phase_2` * molweight3),
      !is.na(`Molality, mol/kg c2 phase_2`) & !is.na(`Solvent: Mass fraction c1 phase_2`) ~
        `Solvent: Mass fraction c1 phase_2` * (1 - `Molality, mol/kg c2 phase_2` * molweight2 / (1000 + `Molality, mol/kg c2 phase_2` * molweight2)),
      !is.na(`Molality, mol/kg c3 phase_2`) & !is.na(`Solvent: Mass fraction c1 phase_2`) ~
        `Solvent: Mass fraction c1 phase_2` * (1 - `Molality, mol/kg c3 phase_2` * molweight3 / (1000 + `Molality, mol/kg c3 phase_2` * molweight3)),
      !is.na(`Molality, mol/kg c1 phase_2`) ~ `Molality, mol/kg c1 phase_2` * molweight1 / (1000 + `Molality, mol/kg c1 phase_2` * molweight1),
    ),
    mass_fraction_c2p2 = case_when(
      !is.na(`Mass fraction c2 phase_2`) ~ `Mass fraction c2 phase_2`,
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(`Mass fraction c1 phase_2`) ~
        `Solvent: Mass fraction c2 phase_2` * (1 - `Mass fraction c1 phase_2`),
      !is.na(`Solvent: Mass fraction c2 phase_2`) & !is.na(`Mass fraction c3 phase_2`) ~
        `Solvent: Mass fraction c2 phase_2` * (1 - `Mass fraction c3 phase_2`),
      !is.na(`Solvent: Mass ratio of component to other component of binary solvent c2 phase_2`) &
        !is.na(`Mass fraction c3 phase_2`) ~
        `Solvent: Mass ratio of component to other component of binary solvent c2 phase_2` /
          (1 + `Solvent: Mass ratio of component to other component of binary solvent c2 phase_2`) * (1 - `Mass fraction c3 phase_2`),
      !is.na(`Solvent: Mass ratio of component to other component of binary solvent c2 phase_2`) &
        !is.na(`Mass fraction c1 phase_2`) ~
        `Solvent: Mass ratio of component to other component of binary solvent c2 phase_2` /
          (1 + `Solvent: Mass ratio of component to other component of binary solvent c2 phase_2`) * (1 - `Mass fraction c1 phase_2`),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~
        `Molality, mol/kg c2 phase_2` * molweight2 /
          (1000 + `Molality, mol/kg c1 phase_2` * molweight1 + `Molality, mol/kg c2 phase_2` * molweight2),
      !is.na(`Molality, mol/kg c2 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~
        `Molality, mol/kg c2 phase_2` * molweight2 /
          (1000 + `Molality, mol/kg c2 phase_2` * molweight2 + `Molality, mol/kg c3 phase_2` * molweight3),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Solvent: Mass fraction c2 phase_2`) ~
        `Solvent: Mass fraction c2 phase_2` * (1 - `Molality, mol/kg c1 phase_2` * molweight1 / (1000 + `Molality, mol/kg c1 phase_2` * molweight1)),
      !is.na(`Molality, mol/kg c3 phase_2`) & !is.na(`Solvent: Mass fraction c2 phase_2`) ~
        `Solvent: Mass fraction c2 phase_2` * (1 - `Molality, mol/kg c3 phase_2` * molweight3 / (1000 + `Molality, mol/kg c3 phase_2` * molweight3)),
      !is.na(`Molality, mol/kg c2 phase_2`) ~ `Molality, mol/kg c2 phase_2` * molweight2 / (1000 + `Molality, mol/kg c2 phase_2` * molweight2),
    ),
    mass_fraction_c3p2 = case_when(
      !is.na(`Mass fraction c3 phase_2`) ~ `Mass fraction c3 phase_2`,
      !is.na(`Solvent: Mass fraction c3 phase_2`) & !is.na(`Mass fraction c2 phase_2`) ~
        `Solvent: Mass fraction c3 phase_2` * (1 - `Mass fraction c2 phase_2`),
      !is.na(`Solvent: Mass fraction c3 phase_2`) & !is.na(`Mass fraction c1 phase_2`) ~
        `Solvent: Mass fraction c3 phase_2` * (1 - `Mass fraction c1 phase_2`),
      !is.na(`Mass ratio of solute to solvent c3 phase_2`) & !is.na(`Mass fraction c2 phase_2`) ~
        `Mass ratio of solute to solvent c3 phase_2` /
          (`Mass ratio of solute to solvent c3 phase_2` + 1) * (1 - `Mass fraction c2 phase_2`),
      !is.na(`Mass ratio of solute to solvent c3 phase_2`) & !is.na(`Mass fraction c1 phase_2`) ~
        `Mass ratio of solute to solvent c3 phase_2` /
          (`Mass ratio of solute to solvent c3 phase_2` + 1) * (1 - `Mass fraction c1 phase_2`),
      !is.na(`Molality, mol/kg c3 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~
        `Molality, mol/kg c3 phase_2` * molweight3 /
          (1000 + `Molality, mol/kg c3 phase_2` * molweight3 + `Molality, mol/kg c2 phase_2` * molweight2),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~
        `Molality, mol/kg c3 phase_2` * molweight3 /
          (1000 + `Molality, mol/kg c1 phase_2` * molweight1 + `Molality, mol/kg c3 phase_2` * molweight3),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Solvent: Mass fraction c3 phase_2`) ~
        `Solvent: Mass fraction c3 phase_2` * (1 - `Molality, mol/kg c1 phase_2` * molweight1 / (1000 + `Molality, mol/kg c1 phase_2` * molweight1)),
      !is.na(`Molality, mol/kg c2 phase_2`) & !is.na(`Solvent: Mass fraction c3 phase_2`) ~
        `Solvent: Mass fraction c3 phase_2` * (1 - `Molality, mol/kg c2 phase_2` * molweight2 / (1000 + `Molality, mol/kg c2 phase_2` * molweight2)),
      !is.na(`Molality, mol/kg c3 phase_2`) ~ `Molality, mol/kg c3 phase_2` * molweight3 / (1000 + `Molality, mol/kg c3 phase_2` * molweight3),
    )
  ) %>%
  mutate(
    mass_fraction_c1p2 = case_when(
      !is.na(mass_fraction_c1p2) ~ mass_fraction_c1p2,
      !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ 1 - mass_fraction_c2p2 - mass_fraction_c3p2
    ),
    mass_fraction_c2p2 = case_when(
      !is.na(mass_fraction_c2p2) ~ mass_fraction_c2p2,
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c3p2) ~ 1 - mass_fraction_c1p2 - mass_fraction_c3p2
    ),
    mass_fraction_c3p2 = case_when(
      !is.na(mass_fraction_c3p2) ~ mass_fraction_c3p2,
      !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c1p2) ~ 1 - mass_fraction_c2p2 - mass_fraction_c1p2
    )
  )

### get mole fraction

tmlframe <- tmlframe %>%
  mutate(
    mole_fraction_c1p2 = case_when(
      !is.na(`Mole fraction c1 phase_2`) ~ `Mole fraction c1 phase_2`,
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ (
        (mass_fraction_c1p2 / molweight1) /
          (mass_fraction_c1p2 / molweight1 + mass_fraction_c2p2 / molweight2 + mass_fraction_c3p2 / molweight3)
      ),
      !is.na(`Solvent: Mole fraction c1 phase_2`) & !is.na(`Mole fraction c2 phase_2`) ~ `Solvent: Mole fraction c1 phase_2` * (1 - `Mole fraction c2 phase_2`),
      !is.na(`Solvent: Mole fraction c1 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~ `Solvent: Mole fraction c1 phase_2` * (1 - `Mole fraction c3 phase_2`),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) & !is.na(`Mole fraction c2 phase_2`) ~
        `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` /
          (1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) * (1 - `Mole fraction c2 phase_2`),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~
        `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2` /
          (1 + `Solvent: Amount ratio of component to other component of binary solvent c1 phase_2`) * (1 - `Mole fraction c3 phase_2`),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~ `Molality, mol/kg c1 phase_2` /
        (`Molality, mol/kg c1 phase_2` + `Molality, mol/kg c2 phase_2` + 1000 / molweight3),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~ `Molality, mol/kg c1 phase_2` /
        (`Molality, mol/kg c1 phase_2` + `Molality, mol/kg c3 phase_2` + 1000 / molweight2),
    ),
    mole_fraction_c2p2 = case_when(
      !is.na(`Mole fraction c2 phase_2`) ~ `Mole fraction c2 phase_2`,
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ (
        (mass_fraction_c2p2 / molweight2) /
          (mass_fraction_c1p2 / molweight1 + mass_fraction_c2p2 / molweight2 + mass_fraction_c3p2 / molweight3)
      ),
      !is.na(`Solvent: Mole fraction c2 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~ `Solvent: Mole fraction c2 phase_2` * (1 - `Mole fraction c1 phase_2`),
      !is.na(`Solvent: Mole fraction c2 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~ `Solvent: Mole fraction c2 phase_2` * (1 - `Mole fraction c3 phase_2`),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~
        `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` /
          (1 + `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) * (1 - `Mole fraction c1 phase_2`),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~
        `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2` /
          (1 + `Solvent: Amount ratio of component to other component of binary solvent c2 phase_2`) * (1 - `Mole fraction c3 phase_2`),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c2 phase_2`) ~ `Molality, mol/kg c2 phase_2` /
        (`Molality, mol/kg c1 phase_2` + `Molality, mol/kg c2 phase_2` + 1000 / molweight3),
      !is.na(`Molality, mol/kg c2 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~ `Molality, mol/kg c2 phase_2` /
        (`Molality, mol/kg c2 phase_2` + `Molality, mol/kg c3 phase_2` + 1000 / molweight1),
      !is.na(`Solvent: Molality, mol/kg c2 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~
        `Solvent: Molality, mol/kg c2 phase_2` / (`Solvent: Molality, mol/kg c2 phase_2` + 1000 / molweight3) * (1 - `Mole fraction c1 phase_2`),
      !is.na(`Solvent: Molality, mol/kg c2 phase_2`) & !is.na(`Mole fraction c3 phase_2`) ~
        `Solvent: Molality, mol/kg c2 phase_2` / (`Solvent: Molality, mol/kg c2 phase_2` + 1000 / molweight1) * (1 - `Mole fraction c3 phase_2`),
    ),
    mole_fraction_c3p2 = case_when(
      !is.na(`Mole fraction c3 phase_2`) ~ `Mole fraction c3 phase_2`,
      !is.na(mass_fraction_c1p2) & !is.na(mass_fraction_c2p2) & !is.na(mass_fraction_c3p2) ~ (
        (mass_fraction_c3p2 / molweight3) /
          (mass_fraction_c1p2 / molweight1 + mass_fraction_c2p2 / molweight2 + mass_fraction_c3p2 / molweight3)
      ),
      !is.na(`Solvent: Mole fraction c3 phase_2`) & !is.na(`Mole fraction c2 phase_2`) ~ `Solvent: Mole fraction c3 phase_2` * (1 - `Mole fraction c2 phase_2`),
      !is.na(`Solvent: Mole fraction c3 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~ `Solvent: Mole fraction c3 phase_2` * (1 - `Mole fraction c1 phase_2`),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) & !is.na(`Mole fraction c1 phase_2`) ~
        `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2` /
          (1 + `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) * (1 - `Mole fraction c1 phase_2`),
      !is.na(`Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) & !is.na(`Mole fraction c2 phase_2`) ~
        `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2` /
          (1 + `Solvent: Amount ratio of component to other component of binary solvent c3 phase_2`) * (1 - `Mole fraction c2 phase_2`),
      !is.na(`Molality, mol/kg c1 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~ `Molality, mol/kg c3 phase_2` /
        (`Molality, mol/kg c1 phase_2` + `Molality, mol/kg c3 phase_2` + 1000 / molweight2),
      !is.na(`Molality, mol/kg c2 phase_2`) & !is.na(`Molality, mol/kg c3 phase_2`) ~ `Molality, mol/kg c3 phase_2` /
        (`Molality, mol/kg c2 phase_2` + `Molality, mol/kg c3 phase_2` + 1000 / molweight1),
    )
  ) %>%
  mutate(
    mole_fraction_c1p2 = case_when(
      !is.na(mole_fraction_c1p2) ~ mole_fraction_c1p2,
      !is.na(mole_fraction_c2p2) & !is.na(mole_fraction_c3p2) ~ 1 - mole_fraction_c2p2 - mole_fraction_c3p2
    ),
    mole_fraction_c2p2 = case_when(
      !is.na(mole_fraction_c2p2) ~ mole_fraction_c2p2,
      !is.na(mole_fraction_c1p2) & !is.na(mole_fraction_c3p2) ~ 1 - mole_fraction_c1p2 - mole_fraction_c3p2
    ),
    mole_fraction_c3p2 = case_when(
      !is.na(mole_fraction_c3p2) ~ mole_fraction_c3p2,
      !is.na(mole_fraction_c2p2) & !is.na(mole_fraction_c1p2) ~ 1 - mole_fraction_c2p2 - mole_fraction_c1p2
    )
  )

tmlframe <- tmlframe %>%
  filter(
    between(mole_fraction_c1p2, -1e-5, 1 + 1e-5),
    between(mole_fraction_c2p2, -1e-5, 1 + 1e-5),
    between(mole_fraction_c3p2, -1e-5, 1 + 1e-5)
  )

## Check distinct rows

tmlframe <- tmlframe %>%
  distinct(
    inchi1, inchi2, inchi3, T_K, P_kPa,
    mole_fraction_c1p2, mole_fraction_c2p2, mole_fraction_c3p2,
    .keep_all = TRUE
  )

## save

tmlframe %>%
  write_parquet(
    .,
    "sle_ternary.parquet"
  )

tml_saved <- read_parquet("sle_ternary.parquet")
tml_saved %>% colnames()
tml_saved %>% summary()

#########################

tml_saved %>%
  filter(
    c1 != c2,
    (
      grepl("C5H14NO", inchi1, ignore.case = TRUE) |
        grepl("C5H14NO", inchi2, ignore.case = TRUE) |
        grepl("C5H14NO", inchi3, ignore.case = TRUE)
    )
  ) %>%
  group_by(c1, c2, c3, inchi1, inchi2, inchi3) %>%
  summarise(
    n = n(),
    x_min = min(mole_fraction_c1p2),
    x_max = max(mole_fraction_c1p2),
    TK_min = min(T_K),
    TK_max = max(T_K),
    PkPa_min = min(P_kPa),
    PkPa_max = max(P_kPa)
  )
