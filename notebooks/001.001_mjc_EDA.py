# %%
%load_ext autoreload
%autoreload 2
# %%
import numpy as np
from bscsi_data.optm_lz.load import load_for_tickers, clean_optionm
tickers = ['MSFT', 'BP']
df = load_for_tickers(tickers).reset_index()
df
# %%
optionid=df.optionid.values[-1000]
df_option = df[df.optionid==optionid].sort_values('date')
df_option

# %%
# The latest date option was traded is specified by “last_date” in OptionMetrics

# %%
# huh this is weird, many were last traded 100's of days before expiry
(df.exdate-df.last_date).mean()
# %%



# %%
df = clean_optionm(df, verbose=True)
df
# %%
