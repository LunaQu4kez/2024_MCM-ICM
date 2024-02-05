import pandas as pd
import statsmodels.api as sm

filename = r'data\ALASKA.csv'
data = pd.read_csv(filename)
df = pd.DataFrame(data)
df = df[df.iloc[:, 1:].any(axis=1)]
df = df.loc[:, (df != 0).any(axis=0)]
sums = df.iloc[1:, 1:].sum()
top_columns = sums.nlargest(10).index
result_df = df[['Time'] + list(top_columns)]
print(result_df)
result_df.set_index('Time', inplace=True)
model = sm.tsa.VAR(result_df)
results = model.fit(maxlags=5)
#print(results.summary())
forecast_input = result_df.iloc[-5:]
forecast = results.forecast(forecast_input.values, steps=1)
forecast_df = pd.DataFrame(forecast, columns=result_df.columns)

print(forecast_df)