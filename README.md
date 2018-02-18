# nastranpy

A library to interact with nastran models.

## Requirements

* python 3.3 (or later)
* numpy

## Installation

```sh
$ pip install -e <nastranpy_folder>
```

## Usage example

Import the model:

```sh
import nastranpy


model = nastranpy.Model()
model.read(['/Users/Alvaro/nastran_model_input/nastran_launcher.dat'])
```

Export the model:

```sh
model.path = '/Users/Alvaro/nastran_model_modified'
model.write()
```

Get help of a given method:

```sh
help(model.cards)
```

Get help of a given card:

```sh
nastranpy.card_help('GRID')
```

Get a single card by its id:

```sh
coord_card = model.coords[45]
grid_card = model.grids[5462]
elem_card = model.elems[234232]
prop_card = model.props[2342]
mat_card = model.mats[4232]
mpc_card_set = model.mpcs[325234]
spc_card_set = model.spcs[234232]
load_card_set = model.loads[234232]
```

Get cards by different ways:

```sh
# Get grids by ids:
grids = [grid for grid in model.cards('grid', [34, 543453, 234233])]

# Get elements by an ID pattern:
elems = [elem for elem in model.cards('elem', ['9', '34', '*', '*', '1-8'])]

# Get all CQUAD4 and CTRIA cards:
elems = [elem for elem in model.cards(['CQUAD4', 'CTRIA3']]

# Get all shell element cards in a set includes:
elems = [elem for elem in model.cards('e2D', includes=['Sp1_Hng_outbd_v04.bdf',
                                                       'Wing-Box_V16.2.bdf'])]
# Get all element card with a given property:
elems = [elem for elem in self.props[400021].child_cards('elem')]

# Get all property card with a given material:
props = [prop for prop in self.mats[10].child_cards('prop')]
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
model.print_cards(model.cards('grid', includes=['BulkData/Sp2_Sprdr_v05.bdf']))
```

Get ID info for a given card type:

```sh
print(model.get_id_info('mpc', detailed=True))
print(model.get_id_slot('grid', 1000))
```

Get shell geometrical info:

```sh
shells_info = {shell.id: (shell.area, shell.normal, shell.centroid) for
               shell in model.cards('e2D')}
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

model.renumber('grid', model.cards('grid', id_list),
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

model.renumber('grid', model.cards('grid', id_list),
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
