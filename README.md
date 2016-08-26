# nastranpy

A library to interact with nastran models.

## Requirements

* python 3.x
* numpy

## Installation

```sh
$ pip install -e <nastranpy_folder>
```

## Usage example

Import the model:

```sh
import nastranpy

model = nastranpy.Model(path='/Users/Alvaro/nastran_model')
model.read(['BulkData/0000additional_cards_from_launcher.bdf',
            'BulkData/3C0733_Sp1_act_v05.bdf',
            'BulkData/3C0734_Sp1_Hng_outbd_v04.bdf',
            'BulkData/3C0748_Sp2_ob_Sprdr_v05.bdf',
            'Loads/3C0748_air_pressure_loads.bdf'])
```

Export the model:

```sh
model.path = '/Users/Alvaro/nastran_model_modified'
model.write()
```

Get a single card by its id:

```sh
coord_card = model.coords[45]
grid_card = model.grids[5462]
elem_card = model.elems[234232]
prop_card = model.props[2342]
mat_card = model.mats[4232]
mpc_card = model.mpcs[325234]
spc_card = model.spcs[234232]
load_card = model.loads[234232]
```

Get cards by different ways:

```sh
grid_ids = [grid.id for grid in
            model.cards_by_id_pattern('grid',
                                      ['9', '34', '*', '*', '*', '*', '1-8'])]
grids = [grid for grid in model.cards_by_id('grid', grid_ids)]
elems = [elem for elem in model.cards_by_tag(['e2D'])]
props = [prop for prop in model.cards_by_type(['prop'])]
pcomps = [prop for prop in model.cards_by_name(['PCOMP', 'PCOMPG'])]
cards_by_include = [card for card in
                    model.cards_by_include(['BulkData/3C0733_Sp1_act_v05.bdf',
                                            'BulkData/3C0734_Sp1_Hng_outbd_v04.bdf',])]
elems_by_prop = [elem for elem in model.elems_by_prop(3311059)]
props_by_mat = [prop for prop in model.props_by_mat(98000009)]
```

Get model info:

```sh
model.info()
```

Write a model summary to a csv file:

```sh
model.print_summary()
```

Write card fields to a csv file:

```sh
model.print_cards(model.cards_by_type(['grid'],
                                      ['BulkData/3C0748_Sp2_ob_Sprdr_v05.bdf']))
```

Get ID info for a given card type:

```sh
print(model.get_id_info('mpc', detailed=True))
print(model.get_id_slot('grid', 1000))
```

Get shell geometrical info:

```sh
shells_info = {shell.id: (shell.area, shell.normal, shell.centroid) for
               shell in model.cards_by_tag(['e2D'])}
```

Renumber cards by correlation:

```sh
correlation = {
    235437: 4703436,
    235438: 4703437,
    235463: 4703462,
    235464: 4703463,
    235465: 4703464,
}

model.renumber('grid', correlation=correlation)
```

Renumber cards by start id and step:

```sh
id_list = [
    235472,
    235473,
    235474,
    235488,
    235489,
    235490,
]

model.renumber('grid', model.cards_by_id('grid', id_list),
               start=4703465, step=5)
```

Renumber cards by an id pattern:

```sh
id_list = [
    235496,
    235497,
    235510,
    235511,
    235512,
    235513,
    235514,
    235515,
]

model.renumber('grid', model.cards_by_id('grid', id_list),
               id_pattern=['9', '34', '*', '*', '*', '*', '1-8'])
```

Extend element by steps:

```sh
model.elems[3612829].extend(2)
```

Extend element by filter:

```sh
model.elems[8048206].extend(tags=['e2D'])
```

Make include self-contained:

```sh
include = model.includes['BulkData/3C0748_Sp2_ob_Sprdr_v05.bdf']
include.make_self_contained()
```

## Contact
Alvaro Sanz Oriz – alvaro.sanz@aernnova.com
