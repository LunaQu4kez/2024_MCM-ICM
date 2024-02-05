import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import minimize
import random

df = pd.read_excel('ln(lgr).xlsx')
pi_j_lis = [random.uniform(0, 1) for _ in range(50)]
theta_boundj = []
for index, row in df.iterrows():
    result = 2 * pi_j_lis[index] * np.exp(-2 * row['a']) * (1 / (np.exp(2 * row['b^2']) - np.exp(row['b^2'])))
    theta_boundj.append(result)
def f(x):
    return norm.pdf(x)
def F(x):
    return norm.cdf(x)
def func(params, x):
    theta, sigma = params
    return F((x - theta) / sigma) * x / sigma * f((x - theta) / sigma)

x = np.array(theta_boundj)
def loss(params):
    y_pred = np.array([func(params, xi) for xi in x])
    return np.mean(y_pred**2)
init_params = np.array([0.5, 0.5])
result = minimize(loss, init_params, method='SLSQP')
best_params = result.x