# nastranpy

A library to interact with nastran models.

## Requirements

* python 3.x
* numpy

## Usage example

Importing the model:

```sh
from nastranpy import Model

files = ['BulkData/0000additional_cards_from_launcher.bdf',
         'BulkData/3C0733_Sp1_act_v05.bdf',
         'BulkData/3C0734_Sp1_Hng_outbd_v04.bdf',
         'BulkData/3C0748_Sp2_ob_Sprdr_v05.bdf',
         'Loads/3C0748_air_pressure_loads.bdf']

model = Model(path='/Users/Alvaro/nastran_model', files=files)
```

## Contact
Álvaro Sanz Oriz – alvaro.sanz@aernnova.com
