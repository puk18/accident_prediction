# High-Resolution Road Vehicle Collision Prediction for Quebec

This repository contains the source code to predict the occurence of an accident within an hour on different road segments in Quebec. The availability of datasets such as road accidents, weather and road networks from Canadian and Quebec government has made it possible to build the prediction model. The trained model can be used to find the risk of accidents at different points in time and space. Model is trained using the labels i.e 1 for the occurrence of the accidents and 0 for non occurrence of accidents. However, the model cannot be used to predict whether an accident will occur or not, as this depends on various factors which cannot be measured easily.

## Folder Structure
- mains: contains the scripts for the hyperparameter tuning, the training and the evaluation of the models
- src: contains the scripts for the generation of the dataset
