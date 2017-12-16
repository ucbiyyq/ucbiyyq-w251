'''
to run this file,

# bokeh serve . --allow-websocket-origin=50.23.70.165:5006

to see viz, in browser go to

http://50.23.70.165:5006/
'''

from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.plotting import Figure

import numpy as np

source = ColumnDataSource(dict(x=[], y=[], avg=[]))

fig = Figure()
fig.line(source=source, x="x", y="y", line_width=2, alpha=.85, color="red")
fig.line(source=source, x="x", y="avg", line_width=2, alpha=.85, color="blue")

ct = 0
sine_sum = 0
def update_data():
    global ct, sine_sum
    ct += 1
    sine = np.sin(ct)
    sine_sum += sine
    new_data = dict(x=[ct], y=[sine], avg=[sine_sum/ct])
    source.stream(new_data, 100)

curdoc().add_root(fig)
curdoc().add_periodic_callback(update_data, 100)