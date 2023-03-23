import numpy as np
from rdkit import Chem
import deepchem as dc
import deepchem.feat as ft
import torch_geometric as pyg
from torch_geometric.data import Data
import os.path as osp
import os
import torch
from torch_geometric.data import Dataset, download_url
import polars as pl
from tqdm import tqdm


def BinaryGraph(InChI1, InChI2):
    """
    Make one graph out of 2 InChI keys for a binary system

    Parameters
    ------------
    InChI1: str
        InChI value of molecule
    InChI2: str 
        InChI value of molecule

    Return
    ------------
    Graph: deepchem.feat.GraphData
        Graph of binary system
    """

    mol1 = Chem.MolFromInchi(InChI1)
    mol2 = Chem.MolFromInchi(InChI2)

    featurizer = ft.PagtnMolGraphFeaturizer()

    graph = featurizer.featurize((mol1, mol2))

    node_features = np.concatenate((graph[0].node_features,
                                    graph[1].node_features))
    edge_features = np.concatenate((graph[0].edge_features,
                                    graph[1].edge_features))
    edge_index = np.concatenate((graph[0].edge_index,
                                 graph[1].edge_index + graph[0].num_nodes),
                                axis=1)

    return ft.GraphData(node_features, edge_index, edge_features)


class MolecularDataset(Dataset):
    def __init__(self, root, transform=None, pre_transform=None, pre_filter=None):
        super().__init__(root, transform, pre_transform, pre_filter)

    @property
    def raw_file_names(self):
        return os.listdir(self.raw_dir)

    @property
    def processed_file_names(self):
        return os.listdir(self.processed_dir)

    def download(self):
        pass

    def process(self):
        idx = 0
        for raw_path in self.raw_paths:
            # Read data from `raw_path`.
            self.dataframe = pl.read_parquet(raw_path)
            for idx, data in tqdm(self.dataframe.rows(), self.dataframe.shape[0]):
                # take info from data and make graphs before saving ahead
                torch.save(data, osp.join(
                    self.processed_dir, f'data_{idx}.pt'))
                idx += 1

    def len(self):
        return len(self.processed_file_names)

    def get(self, idx):
        data = torch.load(osp.join(self.processed_dir, f'data_{idx}.pt'))
        return data
