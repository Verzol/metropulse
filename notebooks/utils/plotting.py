def plot_bar(pdf, x, y, ax=None, color=None, title=None, xlabel=None, ylabel=None, **kwargs):
    """Plot a bar chart from a small aggregate Pandas DataFrame."""
    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots()

    plot_kwargs = dict(kwargs)
    if color is not None:
        plot_kwargs["color"] = color

    ax.bar(pdf[x], pdf[y], **plot_kwargs)
    _apply_labels(ax, title=title, xlabel=xlabel or x, ylabel=ylabel or y)
    return ax


def plot_line(pdf, x, y, ax=None, color=None, title=None, xlabel=None, ylabel=None, **kwargs):
    """Plot a line chart from a small aggregate Pandas DataFrame."""
    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots()

    plot_kwargs = dict(kwargs)
    if color is not None:
        plot_kwargs["color"] = color

    ax.plot(pdf[x], pdf[y], **plot_kwargs)
    _apply_labels(ax, title=title, xlabel=xlabel or x, ylabel=ylabel or y)
    return ax


def plot_hist(pdf, column, bins=30, ax=None, color=None, title=None, xlabel=None, ylabel="Frequency", **kwargs):
    """Plot a histogram from one column of a small aggregate Pandas DataFrame."""
    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots()

    plot_kwargs = dict(kwargs)
    if color is not None:
        plot_kwargs["color"] = color

    ax.hist(pdf[column].dropna(), bins=bins, **plot_kwargs)
    _apply_labels(ax, title=title, xlabel=xlabel or column, ylabel=ylabel)
    return ax


def plot_scatter(pdf, x, y, ax=None, color=None, title=None, xlabel=None, ylabel=None, **kwargs):
    """Plot a scatter chart from a small aggregate Pandas DataFrame."""
    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots()

    plot_kwargs = dict(kwargs)
    if color is not None:
        plot_kwargs["color"] = color

    ax.scatter(pdf[x], pdf[y], **plot_kwargs)
    _apply_labels(ax, title=title, xlabel=xlabel or x, ylabel=ylabel or y)
    return ax


def _apply_labels(ax, title=None, xlabel=None, ylabel=None):
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", labelrotation=45)
    return ax
