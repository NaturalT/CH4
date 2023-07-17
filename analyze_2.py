import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns



Big_boi = pd.read_csv("Big_Boi.csv")


sns.heatmap(Big_boi.corr(numeric_only=True), annot=True )

plt.show()