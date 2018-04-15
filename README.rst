*********
nastranpy
*********

A library to interact with nastran models and results.

Requirements
============

* python 3.6 (or later)
* numpy
* pandas
* cryptography
* pyjwt

Installation
============

Run the following command::

    pip install nastranpy


Usage example: Handling nastran models
======================================

Import the model::

    import nastranpy


    model = nastranpy.bdf.Model()
    model.read(['/Users/Alvaro/nastran_model_input/nastran_launcher.dat'])

Export the model::

    model.path = '/Users/Alvaro/nastran_model_modified'
    model.write()

Get help of a given method::

    help(model.cards)

Get help of a given card::

    nastranpy.card_help('GRID')

Get a single card by its id::

    coord_card = model.coords[45]
    grid_card = model.grids[5462]
    elem_card = model.elems[234232]
    prop_card = model.props[2342]
    mat_card = model.mats[4232]
    mpc_card_set = model.mpcs[325234]
    spc_card_set = model.spcs[234232]
    load_card_set = model.loads[234232]

Get cards by different ways::

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

Get model info::

    model.info()

Write a model summary to a csv file::

    model.print_summary()

Write card fields to a csv file::

    model.print_cards(model.cards('grid', includes=['BulkData/Sp2_Sprdr_v05.bdf']))

Get ID info for a given card type::

    print(model.get_id_info('mpc', detailed=True))
    print(model.get_id_slot('grid', 1000))

Get shell geometrical info::

    shells_info = {shell.id: (shell.area, shell.normal, shell.centroid) for
                   shell in model.cards('e2D')}

Renumber cards by correlation::

    correlation = {
        235437: 4703436,
        235438: 4703437,
        235463: 4703462,
        235464: 4703463,
        235465: 4703464,
    }

    model.renumber('grid', correlation=correlation)

Renumber cards by start id and step::

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


Renumber cards by an id pattern::

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

Extend elements by steps::

    # Extend from an element
    model.elems[3612829].extend(steps=2)
    # Extend from a grid
    model.grids[3815443].extend(steps=2)

Extend elements by filter::

    # Extend from an element
    model.elems[8048206].extend('e2D')
    # Extend from a grid
    model.grids[8020333].extend('e2D')

Make include self-contained::

    include = model.includes['BulkData/3C0748_Sp2_ob_Sprdr_v05.bdf']
    include.make_self_contained()


Usage example: Handling nastran results with a local database
=============================================================

Create a new database ::

    import nastranpy


    files = ['/Users/Alvaro/FEM_results/file01.pch', '/Users/Alvaro/FEM_results/file02.pch']
    database_path = '/Users/Alvaro/databases/FooDatabase'
    database_name = 'Foo database'
    database_version = '0.0.1'

    database = nastranpy.results.Database()
    database.create(files, database_path, database_name, database_version)

Load an existing database ::

    database = nastranpy.results.DataBase(database_path)

Check database integrity ::

    database.check()

Display database info ::

    database.info()

Perform a query::

    query = nastranpy.results.get_query_from_file(query_file)
    dataframe = database.query(**query)

Append new result files to an existing database (this action is reversible)::

    files = ['/Users/Alvaro/FEM_results/file03.pch', '/Users/Alvaro/FEM_results/file04.pch']
    batch_name = 'new_batch'
    database.append(files, batch_name)

Restore database to a previous state (this action is NOT reversible!)::
    database.restore('Initial batch')


Usage example: Handling nastran results with remote databases
=============================================================

Open a new client interfacing the cluster (you will be asked to login)::

    import nastranpy


    client = nastranpy.results.Client(('192.168.0.154', 8080))

Create a new database::

    files = ['/Users/Alvaro/FEM_results/file01.pch', '/Users/Alvaro/FEM_results/file02.pch']
    database_path = 'FooDatabase'
    database_name = 'Foo database'
    database_version = '0.0.1'

    database = nastranpy.results.Database()
    database.create(files, database_path, database_name, database_version)

Load a database::

    client.load('FooDatabase')

Display database info ::

    client.database.info()

Check database integrity ::

    client.database.check()

Perform a query::

    query = nastranpy.results.get_query_from_file(query_file)
    dataframe = client.database.query(**query)

Append new result files to an existing database (this action is reversible)::

    files = ['/Users/Alvaro/FEM_results/file03.pch', '/Users/Alvaro/FEM_results/file04.pch']
    batch_name = 'new_batch'
    client.database.append(files, batch_name)

Restore database to a previous state (this action is NOT reversible!)::

    client.database.restore('Initial batch')

Display cluster info::

    client.info()

List cluster sessions::

    client.list_sessions()

Add a new session::

    client.add_session('jimmy_mcnulty', 'Im_the_boss', is_admin=True)

Remove a session::

    client.remove_session('jimmy_mcnulty')

Remove a database::

    client.remove_database('FooDatabase')

Sync databases between cluster nodes::

    client.sync_databases()

Shutdown the cluster::

    client.shutdown()


Contact
=======
Álvaro Sanz Oriz – alvaro.sanz.oriz@gmail.com
