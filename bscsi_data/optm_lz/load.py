from ..config import path_interim, path_raw
import pandas as pd
from ..helpers import unlz
import numpy as np
import functools
import pyfeng as pf

@functools.lru_cache()
def load_securd1():
    f = path_raw / "optm_lz/securd1.sas7bdat.lz"
    f2 = unlz(f)
    return pd.read_sas(f2)

def get_secid(ticker):
    """ticker -> secid"""
    _df_names = load_securd1()
    secids = _df_names[_df_names.ticker == ticker.encode()].secid
    assert len(secids) == 1, f'cannot find ticker "{ticker}". Found {secids}'
    secid = secids.item()
    return secid


def load_for_tickers(tickers=["MSFT"], *args, **kwargs):
    return pd.concat(load_for_ticker(t, *args, **kwargs) for t in tickers)


def load_for_ticker(ticker="MSFT", filters=[], clean=True):
    secid = get_secid(ticker)
    options_files = sorted(path_interim.glob("optm_lz/opprcd/*.parq"))
    df = pd.concat(
        [
            pd.read_parquet(
                f,
                filters=[
                    ("secid", "==", f"{secid}"),
                    *filters
                    #                           ("cp_flag", "==", "P")
                ],
                engine="pyarrow",
            )
            for f in options_files
        ]
    ).set_index("date")
    df["ticker"] = ticker

    # https://hughchristensen.com/papers/academic_papers/IvyDB%20Europe%20Reference%20Manual.pdf
    df["strike_price"] /= 1000  # seems to be multipled by 1000
    df["dte"] = (df.exdate - df.index).dt.days
    df["mid"] = df[["best_offer", "best_bid"]].mean(1)

    # there is lots of junk put in the wrong symbol
    symbol = df.symbol.str.split(" ").apply(lambda x: x[0])
    symbol_main = symbol.value_counts().index[0]
    assert symbol_main == ticker
    df = df[symbol == symbol_main]

    if clean:
        df = clean_optionm(df)

    return df



def clean_optionm(df, verbose=False):
    # some data cleaning filter from https://www.lsu.edu/business/documents/2019/cross-section-of-individual-equity.pdf
    def apply_mask(df, m, name):
        if verbose:
            print(f"'{name}' kept:{m.mean():2.2%}")
        return df[m]

    # 1.

    # 2. 
    m = df.contract_size==100
    m = m & df.strike_price%0.5==0
    df = apply_mask(df, m, 'best_bid')

    # 3. AM settlement: OptionMetrics “AM_set_flag” is nonzero, meaning that option expires at the market open of the last trading day, rather than market close.
    m = df['am_settlement'] == 0
    df = apply_mask(df, m, 'am_settlement')

    # 4. closing bid quote is missing or not positive
    m = df['best_bid']>0
    df = apply_mask(df, m, 'best_bid')

    # 5. Abnormal bid-ask spread: Bid-ask spread is considered abnormal, if it is: (i) negative, (ii)greater than $5, or (iii) lower than minimum tick size.
    bid_ask_spread = (df['best_offer'] - df['best_bid'])>0
    m = (bid_ask_spread>0.05) & (bid_ask_spread<5)
    df = apply_mask(df, m, 'bid_ask_spread')
    # %%
    # 6. Violates no-arbitrage condition TODO

    # 7. Early excercise TODO

    # 8. Extreme price
    cps = (df.cp_flag=="P")*-2.0 + 1.0 # call, put, switch: 1 for call, -1 for put
    # https://github.com/PyFE/PyFENG/blob/main/pyfeng/bsm.py#L34
    df['model_price'] = pf.Bsm.price_formula(
        strike=df.strike_price, # note this changed some value in place for cps. *1.0 to avoid 
        spot=df.forward_price, 
        sigma=df.impl_volatility, 
        texp=df.dte/365., 
        cp=cps.copy(), 
        is_fwd=True)
    m=(df['mid']-df['model_price'])/df['model_price']>-0.5
    m=m & (df['mid']-df['model_price'])<100
    df = apply_mask(df, m, 'Extreme price')
    # %%
    # 9. Non standard exp
    # FIXME lots of day 4 and some 3? Mean to be third friday of month
    # df.exdate.dt.weekday
    # %%
    # 10. Zero open interest
    m = df['open_interest']>0
    df = apply_mask(df, m, 'Zero open interest')
    # %%
    # # 11. No trade
    # m = df.volume>0
    # df = apply_mask(df, m, 'No trade')
    # df = df[m]
    # %%
    # 12 Abnormal implied vol
    m = df.impl_volatility>0
    df = apply_mask(df, m, 'Abnormal implied vol')
    # %%
    # 13 Abnormal delta: The option delta calculated by Ivy DB is either missing or outside of therange [0,1] for call option and [-1,0] for put option.
    # %%
    cps = (df.cp_flag=="P")*-2.0 + 1.0 # call, put, switch: 1 for call, -1 for put
    deltac = df.delta * cps
    m = (deltac>=0.) & (deltac<=1.)
    df = apply_mask(df, m, 'Abnormal delta')

    # 14 Underlying price history was not found in CRSP database
    # N/A

    return df
