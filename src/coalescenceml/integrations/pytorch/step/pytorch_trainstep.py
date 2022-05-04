from typing import Any, Dict, List, Optional, Tuple, Type
from coalescenceml.step.base_step_config import BaseStepConfig
import numpy as np
import torch
import torch.nn as nn
from coalescenceml.step.base_step import BaseStep 
from torch.utils.data import DataLoader, TensorDataset



class PTClassifierConfig(BaseStepConfig):
    layers: List
    input_shape: Tuple[int, ...] = (28,28) # MNIST Size
    num_classes: int = 2
    learning_rate: float = 1e-3
    epochs: int = 10
    batch_size: int = 32
        
class PyTorchClassifierTrainStep(BaseStep):
    # Note to users: layers parameter shouldn't have 1 as last - it is automatically done
    # an out of box fully-connected NN with linear layers only + relu activation
    def entrypoint(
        self,
        config : PTClassifierConfig,
        x_train: np.ndarray,
        y_train: np.ndarray,
        #x_validation: np.ndarray = np.array([]),
        #y_validation: np.ndarray = np.array([]),
    ) -> nn.Module:
        
        # model creation

        modules = []
        modules.append(nn.Flatten())
        prev_layer = len(x_train[0, :].flatten())
        for i in range(len(config.layers)):
            modules.append(nn.Linear(prev_layer, config.layers[i]))
            modules.append(nn.ReLU())
            prev_layer = config.layers[i]
        
        last_layer = config.num_classes if config.num_classes > 2 else 1
        modules.append(nn.Linear(prev_layer, last_layer))

        if last_layer == 1:
            modules.append(nn.Sigmoid())
            loss_fn = nn.BCELoss(reduction='sum')  
        else:
            modules.append(nn.Softmax())
            loss_fn = nn.CrossEntropyLoss()

        model = nn.Sequential(*modules)

        # data building
        dataSet = TensorDataset(torch.Tensor(x_train) , torch.Tensor(y_train))
        train_loader = DataLoader(dataSet, batch_size=config.batch_size, shuffle=True)
        
        #train
        optimizer = torch.optim.SGD(model.parameters(), lr=config.learning_rate, momentum=0.9)
        model.train()
        
        for i in range(config.epochs):
            for X_batch, y_batch in train_loader:
                y_pred = model(X_batch)
                y_batch = torch.reshape(y_batch, y_pred.shape)

                # Compute and print loss. We pass Tensors containing the predicted and true
                # values of y, and the loss function returns a Tensor containing the
                # loss.
                loss = loss_fn(y_pred, y_batch)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
        

        return model

                

         