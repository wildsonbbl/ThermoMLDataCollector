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

## checking type of data

tmlset %>%
    select(type) %>%
    as.data.frame() %>%
    distinct()
## selecting properties of interest

properties <- c(
    "Mass density, kg/m3",
    "Vapor or sublimation pressure, kPa",
    "Activity coefficient"
)

tmlframe <- tmlset %>%
    filter(
        type %in% properties,
        is.na(c3),
        is.na(phase_3)
    ) %>%
    select(
        fl = filename, c1, c2, inchi1, inchi2, phase_1, phase_2,
        TK = `Temperature, K`, m0, m1, m2, m0_phase, type,
        mlc1p1 = `Mole fraction c1 phase_1`,
        mlc2p1 = `Mole fraction c2 phase_1`,
        mlc2p2 = `Mole fraction c2 phase_2`,
        mlc1p2 = `Mole fraction c1 phase_2`,
        PkPA = `Pressure, kPa`
    ) %>%
    as.data.frame()

## analysis

tmlframe %>% nrow()
tmlframe %>% summary()

### Numerating properties

tmlframe <- tmlframe %>% mutate(
    tp = if_else(
        type == "Mass density, kg/m3",
        1,
        NA
    ),
    tp = if_else(
        type == "Activity coefficient",
        2,
        tp
    ),
    tp = if_else(
        type == "Vapor or sublimation pressure, kPa",
        3,
        tp
    )
)

### Fill in missing mole fraction info

tmlframe <- tmlframe %>%
    mutate(mlc1p1 = if_else(
        is.na(mlc1p1),
        1 - mlc2p1,
        mlc1p1
    ))

tmlframe <- tmlframe %>%
    mutate(mlc2p1 = if_else(
        is.na(mlc2p1),
        1 - mlc1p1,
        mlc2p1
    ))

tmlframe <- tmlframe %>%
    mutate(mlc1p2 = if_else(
        is.na(mlc1p2),
        1 - mlc2p2,
        mlc1p2
    ))

tmlframe <- tmlframe %>%
    mutate(mlc2p2 = if_else(
        is.na(mlc2p2),
        1 - mlc1p2,
        mlc2p2
    ))

### fill in c2

tmlframe <- tmlframe %>% mutate(
    c2 = if_else(
        is.na(c2),
        c1,
        c2
    ),
    inchi2 = if_else(
        is.na(inchi2),
        inchi1,
        inchi2
    )
)

### rename and merge mole fraction

tmlframe <- tmlframe %>% mutate(
    mlc1 = if_else(
        is.na(mlc1p1),
        mlc1p2,
        mlc1p1
    ),
    mlc2 = if_else(
        is.na(mlc2p1),
        mlc2p2,
        mlc2p1
    )
)

tmlframe <- tmlframe %>%
    mutate(
        mlc1 = if_else(
            c1 == c2,
            1.0,
            mlc1
        ),
        mlc2 = if_else(
            c1 == c2,
            0.0,
            mlc2
        )
    )

tmlframe <- tmlframe %>%
    filter(!is.na(mlc1))

### checking and filtering phases

tmlframe %>%
    group_by(phase_1, phase_2) %>%
    summarise(n = n()) %>%
    arrange(desc(n))

tmlframe %>%
    group_by(m0_phase) %>%
    summarise(n = n()) %>%
    arrange(desc(n))

tmlframe <- tmlframe %>% filter(
    m0_phase %in% c("Gas", "Liquid", NA),
    phase_1 %in% c("Gas", "Liquid", NA),
    phase_2 %in% c("Gas", "Liquid", NA)
)

tmlframe %>%
    filter(is.na(m0_phase)) %>%
    summary()

tmlframe <- tmlframe %>% mutate(
    phase = if_else(
        m0_phase == "Gas",
        0.0,
        1.0,
        1.0
    )
)

### Checking and filtering pressure and temperature

tmlframe %>%
    filter(type == "Vapor or sublimation pressure, kPa") %>%
    summary()
tmlframe %>%
    filter(type == "Mass density, kg/m3") %>%
    summary()
tmlframe %>%
    filter(type == "Activity coefficient") %>%
    summary()

tmlframe <- tmlframe %>%
    mutate(
        PPa = if_else(
            type == "Vapor or sublimation pressure, kPa",
            m0 * 1000.0,
            PkPA * 1000.0
        ),
        PPa = if_else(
            is.na(PPa) & type == "Activity coefficient",
            101325.0,
            PPa
        )
    ) %>%
    filter(!is.na(PPa))


tmlframe %>%
    filter(TK > 600 + 273.15) %>%
    arrange(TK) %>%
    group_by(type) %>%
    summarise(n = n())

tmlframe %>%
    filter(TK > 600 + 273.15) %>%
    arrange(PPa) %>%
    view()

### kPa vapor pressure to Pa

tmlframe <- tmlframe %>% mutate(
    m = if_else(
        tp == 3,
        m0 * 1000.0,
        m0
    )
)

tmlframe %>% summary()

## dividing between pure and binary datasets

pure <- tmlframe %>%
    filter(c1 == c2) %>%
    select(
        where(
            ~ !all(is.na(.x))
        )
    )

binary <- tmlframe %>%
    filter(c1 != c2) %>%
    select(
        where(
            ~ !all(is.na(.x))
        )
    )

pure %>% summary()

### checking molecules available

pure %>%
    distinct(inchi1, inchi2) %>%
    nrow()

binary %>%
    distinct(inchi1, inchi2) %>%
    nrow()

### checking properties present in each dataset

pure %>%
    group_by(type) %>%
    summarise(n = n())

pure %>%
    filter(tp == 1) %>%
    summary()

pure %>%
    filter(tp == 3) %>%
    summary()

binary %>%
    group_by(type) %>%
    summarise(n = n())

pure %>% summary()

pure %>% filter(PkPA == 0)

binary %>% summary()

### merging binary data into single column
binary <- binary %>% mutate(
    m = if_else(
        !is.na(m1) & is.na(m0),
        m1,
        m
    ),
    m = if_else(
        !is.na(m2) & is.na(m0),
        m2,
        m
    )
)

binary %>% distinct(phase)

binary <- binary %>% select(
    c1, c2, inchi1, inchi2, mlc1, mlc2, TK, PPa, phase, tp, m
)

binary %>% summary()


binary %>%
    select(inchi1, inchi2) %>%
    pivot_longer(cols = c(inchi1, inchi2)) %>%
    distinct(value) %>%
    nrow() +
    pure %>%
    distinct(inchi1) %>%
    nrow() -
    tmlframe %>%
    select(inchi1, inchi2) %>%
    pivot_longer(cols = c(inchi1, inchi2)) %>%
    distinct(value) %>%
    nrow()
