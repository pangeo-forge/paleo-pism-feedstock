# We will concatenate over the ensemble number (as a first attempt)

# create identification numbers 
ids = list(range(6000,6256))

# use the ensemble numbers to make a ConcatDim
from pangeo_forge_recipes.patterns import ConcatDim
id_concat_dim = ConcatDim("id", ids, nitems_per_file=1)

# define a function for making the URL
def make_url(id):
    return f"zip://geo_data/pism1.0_paleo06_{id}/geometry_paleo_1ka.nc::https://download.pangaea.de/dataset/940149/files/paleo_ensemble_geo_2022.zip"

# make a FilePattern
from pangeo_forge_recipes.patterns import FilePattern
pattern = FilePattern(make_url, id_concat_dim)

import fsspec
import pandas as pd
of = fsspec.open("zip://geo_data/aggregated_data/pism1.0_paleo06_6000.csv::https://download.pangaea.de/dataset/940149/files/paleo_ensemble_geo_2022.zip")
with of as f:
    df = pd.read_csv(f,sep="\s", engine='python')


def add_id_and_pars(ds, fname):
    import numpy as np
    import time

    id_str = fname[31:35]
    id_num = np.array([id_str])
    ds = ds.expand_dims("id").assign_coords(id=("id",id_num)) 
    
    wait_time = 1.0
    print(f"sleeping for {wait_time} s.")
    time.sleep(wait_time)

    ds = ds.assign_coords({"par_esia":  ('id', [df.sia_e[df['ens_member'].str.contains(id_str)][0]] ), 
                            "par_ppq":   ('id', [df.ppq[df['ens_member'].str.contains(id_str)][0]] ),
                            "par_prec":  ('id', [df.prec[df['ens_member'].str.contains(id_str)][0]] ),
                            "par_visc":  ('id', [df.visc[df['ens_member'].str.contains(id_str)][0]])
                            } 
                           )

    return ds


# make a recipe using the ConcatDim and FilePattern objects we just made
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
recipe = XarrayZarrRecipe(pattern, 
                          inputs_per_chunk=1,
                          process_input=add_id_and_pars,
                          cache_inputs = 'False')    # one per chunk because each nc is ~210 MB
