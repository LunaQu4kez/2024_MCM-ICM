import numpy as np
from scipy.optimize import least_squares

# Define the function to be fitted
def func(params, x):
    a, b = params
    return a * x + b

# Defining Observations
x = np.array([1, 2, 3, 4, 5])
y_observed = np.array([4.8, 8.5, 10.3, 13.8, 17.2])

# Define objective function (loss function)
def objective(params, x, y_observed):
    return func(params, x) - y_observed

# Initial value of initialization parameters
initial_params = [1, 1]

# Fitting using the least squares method
result = least_squares(objective, initial_params, 
args=(x, y_observed))

# Obtain fitted parameters
a_opt, b_opt = result.x

print("Fitting results:")
print(f"a: {a_opt}")
print(f"b: {b_opt}")