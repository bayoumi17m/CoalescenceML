class SKLearnTrainStep(BaseStep):
    '''DOCSTRING'''

    def entrypoint(self, x: np.ndarray, y: np.ndarray, model_name: typing.AnyStr, hyperparams: typing.Dict = {}) -> BaseEstimator:
        # Possibly change model_type to a classifier model instead of string
        # Possibly log experimental results 
        # Possibly add validation data as optional too

        # Assert x and y have the same number of inputs
        assert(x.shape[0] == y.shape[0])
    
        # Create model based on model_name input
        if model_name == 'knn':
          model = KNeighborsClassifier(**hyperparams)
        elif model_name == 'svm':
          model = SVC(**hyperparams)
        elif model_name == 'linear_reg':
          model = LinearRegression(**hyperparams)
        elif model_name == 'logistic_reg':  
          model = LogisticRegression(**hyperparams)
        elif model_name == 'decision_tree':  
          model = tree.DecisionTreeClassifier(**hyperparams)
        elif model_name == 'random_forest':  
          model = RandomForestClassifier(**hyperparams)
        elif model_name == 'gaussian_nb':  
          model = GaussianNB(**hyperparams)

        # Train model
        model.fit(x, y)

        return model