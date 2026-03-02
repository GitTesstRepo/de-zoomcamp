import marimo as mo

app = mo.App()


@app.cell
def _():
    "Configuration"
    db_path = r"dlt_open_library_pipeline\open_library_pipeline.duckdb"
    return db_path


@app.cell
def _(db_path):
    # Connect directly to DuckDB database created by the dlt pipeline
    import duckdb

    con = duckdb.connect(db_path)
    return con


@app.cell
def _(con):
    # Compute top 10 authors by distinct book count using DuckDB SQL.
    top_authors = con.execute(
        """
        SELECT
          an.value AS author,
          COUNT(DISTINCT b._dlt_id) AS book_count
        FROM open_library_pipeline_dataset.harry_potter_books b
        JOIN open_library_pipeline_dataset.harry_potter_books__author_name an
          ON an._dlt_parent_id = b._dlt_id
        GROUP BY author
        ORDER BY book_count DESC
        LIMIT 10
        """
    ).df()

    # Compute number of books over time (by first_publish_year).
    books_over_time = con.execute(
        """
        SELECT
          first_publish_year AS year,
          COUNT(*) AS book_count
        FROM open_library_pipeline_dataset.harry_potter_books
        WHERE first_publish_year IS NOT NULL
        GROUP BY year
        ORDER BY year
        """
    ).df()

    return top_authors, books_over_time


@app.cell
def _(top_authors, books_over_time):
    try:
        import matplotlib.pyplot as plt  # type: ignore[import-not-found]
    except ImportError:
        import marimo as mo

        mo.md(
            "Matplotlib is not installed in this environment.\n\n"
            "Install it (e.g. `uv add matplotlib`) to see the bar chart, "
            "or use the table above for the top authors."
        )
    else:
        import marimo as mo

        # Horizontal bar chart of top 10 authors by book count
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(top_authors["author"], top_authors["book_count"])
        ax.invert_yaxis()
        ax.set_xlabel("Number of books")
        ax.set_title("Top 10 authors by Harry Potter book count")
        fig.tight_layout()
        mo.display(fig)

        # Line chart: books over time by first_publish_year
        fig2, ax2 = plt.subplots(figsize=(10, 6))
        ax2.plot(books_over_time["year"], books_over_time["book_count"], marker="o")
        ax2.set_xlabel("First publish year")
        ax2.set_ylabel("Number of books")
        ax2.set_title("Harry Potter books over time")
        fig2.tight_layout()
        mo.display(fig2)

    # Also show the underlying tables
    top_authors
    books_over_time
    return


if __name__ == "__main__":
    app.run()
