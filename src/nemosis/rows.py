import tkinter as tk
import tkinter.ttk as ttk
from nemosis import defaults


class Query:
    def __init__(self, master, row_number, app, table_options=None):
        # Load in the starting features of a query row.
        self.master = master
        self.row_number = row_number

        # Make an label and entry box for the user to name the query result.
        self.query_label = ttk.Label(self.master, text="  Query name:")
        self.name = ttk.Entry(self.master)
        self.name.config(width=26)

        # Make labels and entry boxes for the user to provide start and end time to filter the query based on.
        self.start_time_label = ttk.Label(
            self.master, text="Start time:\n(YYYY/MM/DD HH:MM:SS)"
        )
        self.start_time = ttk.Entry(self.master)
        self.start_time.config(width=26)
        self.end_time_label = ttk.Label(
            self.master, text="End time:\n(YYYY/MM/DD HH:MM:SS)"
        )
        self.end_time = ttk.Entry(self.master)
        self.end_time.config(width=26)

        # Create a label and a list of tables to choose from.
        self.tables_label = ttk.Label(self.master, text="Select table:")
        self.tables = tk.Listbox(self.master, exportselection=False, width=35)
        self.tables.bind("<<ListboxSelect>>", self.add_column_selection)

        self.table_options = table_options

        for item in table_options:
            self.tables.insert(tk.END, item)

        # Create a button to delete the row.
        self.delete = ttk.Button(
            self.master, text="\u274C", command=lambda: app.delete_row(self.row_number)
        )

        # Create empty attributes to fill up later on.
        self.filter_list = {}
        self.filter_label = {}
        self.filter_entry = {}
        self.col_list = None
        self.cols_label = None

        # Position all the widgets in the row.
        self.position()

    def position(self):
        first_sub_column = 0
        second_sub_column = 1
        pady = defaults.query_y_pad
        padx = defaults.standard_x_pad

        self.query_label.grid(
            row=defaults.query_row_offset + defaults.row_height * self.row_number,
            column=first_sub_column,
            pady=pady,
            padx=padx,
            sticky="sw",
        )
        self.query_label.update()

        self.name.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.names_internal_row,
            column=first_sub_column,
            padx=padx,
            sticky="sw",
        )
        self.name.update()

        self.start_time_label.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_label_internal_row,
            column=first_sub_column,
            padx=defaults.standard_x_pad,
            sticky="sw",
        )
        self.start_time_label.update()

        self.start_time.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_internal_row,
            column=first_sub_column,
            padx=padx,
            sticky="sw",
        )
        self.start_time.update()

        self.end_time_label.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.end_time_label_internal_row,
            column=first_sub_column,
            padx=padx,
            sticky="sw",
        )
        self.end_time_label.update()

        self.end_time.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.end_time_internal_row,
            column=first_sub_column,
            padx=padx,
            sticky="sw",
        )
        self.end_time.update()

        self.tables_label.grid(
            row=defaults.query_row_offset + defaults.row_height * self.row_number,
            column=second_sub_column,
            pady=defaults.query_y_pad,
            sticky="sw",
            padx=padx,
        )
        self.tables_label.update()

        self.tables.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.table_list_internal_row,
            column=second_sub_column,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="sw",
            padx=padx,
        )
        self.tables.update()

        self.delete.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.delete_button_internal_row,
            column=defaults.last_column,
            sticky="nw",
        )
        self.delete.update()

        if self.col_list is not None:
            self.position_column_list()
        if self.filter_list:
            self.position_filter_list()

    def add_column_selection(self, evt):
        # When a table is selected update the list of columns to be selected from.
        # Find the name of the table selected.
        table = self.table_options[self.tables.curselection()[0]]
        # Delete the previous list of columns.
        if self.col_list is not None:
            self.col_list.destroy()
            self.cols_label.destroy()

        # Create a new label and list box.
        self.cols_label = ttk.Label(self.master, text="Select columns:")
        self.col_list = tk.Listbox(
            self.master, selectmode=tk.MULTIPLE, exportselection=False, width=26
        )
        self.col_list.bind("<<ListboxSelect>>", self.add_filters)
        self.col_list.delete(0, tk.END)

        # Populate the list box with column names.
        for item in defaults.table_columns[table]:
            self.col_list.insert(tk.END, item)

        for col in defaults.table_primary_keys[table]:
            index = defaults.table_columns[table].index(col)
            self.col_list.selection_set(index)

        # Position the column list.
        self.position_column_list()

        # Delete any filters that existed for previous column selections.
        self.remove_filters()
        self.add_filters(None)

    def position_column_list(self):
        self.cols_label.grid(
            column=self.tables.grid_info()["column"] + defaults.list_column_span,
            row=defaults.query_row_offset + self.row_number * defaults.row_height,
            sticky="sw",
        )
        self.cols_label.update()
        self.col_list.grid(
            column=self.tables.grid_info()["column"] + defaults.list_column_span,
            row=defaults.query_row_offset
                + self.row_number * defaults.row_height
                + defaults.table_list_internal_row,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            padx=defaults.standard_x_pad,
            sticky="sw",
        )
        self.col_list.update()

    def add_filters(self, evt):
        # When a column is selected update the filter options.

        # Remove filters for columns that are no longer selected.
        self.remove_filters_unselected()

        # Find which columns are currently selected.
        select_cols = [
            self.col_list.get(0, tk.END)[index]
            for index in self.col_list.curselection()
        ]

        # If a column is selected, and is filterable, but does not have a filter then add a filter for that column.
        for column in select_cols:
            if (
                column in defaults.filterable_cols
                and column not in self.filter_label.keys()
            ):
                self.filter_label[column] = ttk.Label(
                    self.master, text="Select {}s:".format(str(column))
                )
                self.filter_entry[column] = ttk.Entry(self.master, width=25)
                self.filter_entry[column].bind("<Return>", self.add_to_list)
                self.filter_entry[column].name = column
                self.filter_list[column] = tk.Listbox(
                    self.master,
                    selectmode=tk.MULTIPLE,
                    exportselection=False,
                    height=8,
                    width=25,
                )

        # Position all the filters so there are no gaps in between them.
        self.position_filter_list()

    def position_filter_list(self):
        last_filter = None
        # Position all the filter next to each other moving left across the screen.
        for column in self.filter_label.keys():
            if last_filter is None:
                # Place the first filter next to the column list.
                col = self.col_list.grid_info()["column"] + defaults.list_column_span
            else:
                # Place the next filter next to the last filter.
                col = (
                        self.filter_label[last_filter].grid_info()["column"]
                        + defaults.list_column_span
                )

            self.filter_label[column].grid(
                row=defaults.query_row_offset + defaults.row_height * self.row_number,
                column=col,
                padx=defaults.standard_x_pad,
                sticky="sw",
            )
            self.filter_label[column].update()
            self.filter_entry[column].grid(
                row=defaults.query_row_offset
                    + defaults.row_height * self.row_number
                    + defaults.names_internal_row,
                column=col,
                padx=defaults.standard_x_pad,
            )
            self.filter_entry[column].update()
            self.filter_list[column].grid(
                row=defaults.query_row_offset
                    + defaults.row_height * self.row_number
                    + defaults.internal_filter_row,
                column=col,
                columnspan=defaults.list_column_span,
                rowspan=defaults.list_filter_row_span,
                padx=defaults.standard_x_pad,
            )
            self.filter_list[column].update()
            last_filter = column

    def add_to_list(self, evt):
        # Add the item in the entry box of a filter to the list box below.
        self.filter_list[evt.widget.name].insert(tk.END, evt.widget.get())
        evt.widget.delete(0, "end")

    def remove_filters_unselected(self):
        # Delete filter whoes columns are not selected.
        select_cols = [
            self.col_list.get(0, tk.END)[index]
            for index in self.col_list.curselection()
        ]
        existing_filters = list(self.filter_label.keys())
        for column in existing_filters:
            if column not in select_cols:
                self.filter_label[column].destroy()
                del self.filter_label[column]
                self.filter_entry[column].destroy()
                del self.filter_entry[column]
                self.filter_list[column].destroy()
                del self.filter_list[column]

    def empty(self):
        # Delete all the widgets in the row.
        if self.filter_list:
            # Delete the filters.
            self.remove_filters()
        if self.col_list is not None:
            # Delete the column list.
            self.col_list.destroy()
            del self.col_list
        if self.cols_label is not None:
            self.cols_label.destroy()
            del self.cols_label
        # Delete the the row features that always exist.
        self.remove_initial_features()

    def remove_initial_features(self):
        # Remove the initial widegts.
        self.query_label.destroy()
        del self.query_label
        self.name.destroy()
        del self.name
        self.start_time_label.destroy()
        del self.start_time_label
        self.start_time.destroy()
        del self.start_time
        self.end_time_label.destroy()
        del self.end_time_label
        self.end_time.destroy()
        del self.end_time
        self.tables_label.destroy()
        del self.tables_label
        self.tables.destroy()
        del self.tables
        self.delete.destroy()
        del self.delete

    def remove_filters(self):
        # Remove any filter widgets that exist.
        existing_filters = list(self.filter_label.keys())
        for column in existing_filters:
            self.filter_label[column].destroy()
            del self.filter_label[column]
            self.filter_entry[column].destroy()
            del self.filter_entry[column]
            self.filter_list[column].destroy()
            del self.filter_list[column]

    def state(self):
        # Return the current state of the row as a dictionary.
        state = {}
        state["type"] = "query"
        state["name"] = self.name.get()
        state["start_time"] = self.start_time.get()
        state["end_time"] = self.end_time.get()
        if len(self.tables.curselection()) != 0:
            state["table"] = self.table_options[self.tables.curselection()[0]]
        if self.col_list is not None:
            state["columns"] = [
                self.col_list.get(0, tk.END)[index]
                for index in self.col_list.curselection()
            ]
        state["filters_contents"] = {}
        state["filters_selection"] = {}
        for column, filter_list in self.filter_list.items():
            state["filters_contents"][column] = self.filter_list[column].get(0, tk.END)
            state["filters_selection"][column] = self.filter_list[column].curselection()
        return state

    def load_state(self, state):
        # Update the row to match the state provided.
        self.name.insert(0, state["name"])
        self.start_time.insert(0, state["start_time"])
        self.end_time.insert(0, state["end_time"])
        if len(state["table"]) != 0:
            table_index = list(self.tables.get(0, "end")).index(state["table"])
            self.tables.selection_set(table_index)
            self.add_column_selection(None)
            for col in state["columns"]:
                col_index = list(self.col_list.get(0, "end")).index(col)
                self.col_list.selection_set(col_index)
            self.add_filters(None)
        for column, filter_contents in state["filters_contents"].items():
            self.filter_list[column].insert(0, *filter_contents)
        for column, filter_selection in state["filters_selection"].items():
            for index in filter_selection:
                self.filter_list[column].selection_set(index)


class Merge_as_of:
    def __init__(self, master, row_number, app):
        # Create all the widgets of a merge row.
        self.master = master
        self.row_number = row_number
        # Create a label and entry box to name the result of the merge
        self.merge_label = ttk.Label(self.master, text="Merge name")
        self.name = ttk.Entry(self.master)
        self.name.config(width=26)
        # Create entry box to provide the name of the left result to merge.
        self.left_table_label = ttk.Label(self.master, text="Left table")
        self.left_table = ttk.Entry(self.master)
        self.left_table.config(width=26)
        # Create an entry box to provide the name of the right result to merge.
        self.right_table_label = ttk.Label(self.master, text="Right table")
        self.right_table = ttk.Entry(self.master)
        self.right_table.config(width=26)
        # Create a list to select the merge type from.
        self.join_types_label = ttk.Label(self.master, text="Select join type")
        self.join_types = tk.Listbox(self.master, exportselection=False, width=28)
        for item in defaults.join_type:
            self.join_types.insert(tk.END, item)
        # Create a button that deletes the row.
        self.delete = ttk.Button(
            self.master, text="\u274C", command=lambda: app.delete_row(self.row_number)
        )
        # Create a entry box and list to provide the keys to the left result.
        self.left_time_keys_label = ttk.Label(self.master, text="Left time keys")
        self.left_time_keys_entry = ttk.Entry(self.master)
        self.left_time_keys_entry.bind("<Return>", self.add_to_list_left_time)
        self.left_time_key_list = tk.Listbox(
            self.master, selectmode=tk.MULTIPLE, exportselection=False, height=8
        )
        # Create a entry box and list to provide the keys to the right result.
        self.right_time_keys_label = ttk.Label(self.master, text="Right time keys")
        self.right_time_keys_entry = ttk.Entry(self.master)
        self.right_time_keys_entry.bind("<Return>", self.add_to_list_right_time)
        self.right_time_key_list = tk.Listbox(
            self.master, selectmode=tk.MULTIPLE, exportselection=False, height=8
        )
        # Create a entry box and list to provide the keys to the left result.
        self.left_keys_label = ttk.Label(self.master, text="Left keys")
        self.left_keys_entry = ttk.Entry(self.master)
        self.left_keys_entry.bind("<Return>", self.add_to_list_left)
        self.left_key_list = tk.Listbox(
            self.master, selectmode=tk.MULTIPLE, exportselection=False, height=8
        )
        # Create a entry box and list to provide the keys to the right result.
        self.right_keys_label = ttk.Label(self.master, text="Right keys")
        self.right_keys_entry = ttk.Entry(self.master)
        self.right_keys_entry.bind("<Return>", self.add_to_list_right)
        self.right_key_list = tk.Listbox(
            self.master, selectmode=tk.MULTIPLE, exportselection=False, height=8
        )
        # Position all the widgets.
        self.position()

    def position(self):
        self.merge_label.grid(
            row=defaults.query_row_offset + defaults.row_height * self.row_number,
            column=0,
            pady=defaults.query_y_pad,
            padx=defaults.standard_x_pad,
            sticky="ws",
        )
        self.merge_label.update()
        self.name.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.names_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.name.update()
        self.left_table_label.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_label_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.left_table_label.update()
        self.left_table.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.left_table.update()
        self.right_table_label.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.end_time_label_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.right_table_label.update()
        self.right_table.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.end_time_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.right_table.update()
        self.join_types_label.grid(
            row=defaults.query_row_offset + defaults.row_height * self.row_number,
            column=1,
            pady=defaults.query_y_pad,
            sticky="sw",
            padx=defaults.standard_x_pad,
        )
        self.join_types_label.update()
        self.join_types.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.table_list_internal_row,
            column=1,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.join_types.update()
        self.delete.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.delete_button_internal_row,
            column=defaults.last_column,
            sticky="nw",
        )
        self.delete.update()

        label_row = defaults.query_row_offset + defaults.row_height * self.row_number
        label_sticky = "w"
        entry_row = (
                defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.names_internal_row
        )
        custom_list_row = (
                defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.internal_filter_row
        )

        left_time_keys_col = (
                self.join_types.grid_info()["column"] + defaults.list_column_span
        )
        self.left_time_keys_label.grid(
            row=label_row,
            column=left_time_keys_col,
            sticky=label_sticky,
            padx=defaults.standard_x_pad,
        )
        self.left_time_keys_entry.grid(
            row=entry_row,
            column=left_time_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.left_time_key_list.grid(
            row=custom_list_row,
            column=left_time_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )

        right_time_keys_col = (
                self.left_time_key_list.grid_info()["column"] + defaults.list_column_span
        )
        self.right_time_keys_label.grid(
            row=label_row,
            column=right_time_keys_col,
            sticky=label_sticky,
            padx=defaults.standard_x_pad,
        )
        self.right_time_keys_entry.grid(
            row=entry_row,
            column=right_time_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.right_time_key_list.grid(
            row=custom_list_row,
            column=right_time_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )

        left_keys_col = (
                self.right_time_key_list.grid_info()["column"] + defaults.list_column_span
        )
        self.left_keys_label.grid(
            row=label_row,
            column=left_keys_col,
            sticky=label_sticky,
            padx=defaults.standard_x_pad,
        )
        self.left_keys_entry.grid(
            row=entry_row,
            column=left_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.left_key_list.grid(
            row=custom_list_row,
            column=left_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )

        right_keys_col = (
                self.left_key_list.grid_info()["column"] + defaults.list_column_span
        )
        self.right_keys_label.grid(
            row=label_row,
            column=right_keys_col,
            sticky=label_sticky,
            padx=defaults.standard_x_pad,
        )
        self.right_keys_entry.grid(
            row=entry_row,
            column=right_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.right_key_list.grid(
            row=custom_list_row,
            column=right_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )

    def add_to_list_left_time(self, evt):
        # Add key from entry box to list.
        self.left_time_key_list.insert(tk.END, evt.widget.get())
        evt.widget.delete(0, "end")

    def add_to_list_right_time(self, evt):
        # Add key from entry box to list.
        self.right_time_key_list.insert(tk.END, evt.widget.get())
        evt.widget.delete(0, "end")

    def add_to_list_left(self, evt):
        # Add key from entry box to list.
        self.left_key_list.insert(tk.END, evt.widget.get())
        evt.widget.delete(0, "end")

    def add_to_list_right(self, evt):
        # Add key from entry box to list.
        self.right_key_list.insert(tk.END, evt.widget.get())
        evt.widget.delete(0, "end")

    def empty(self):
        # Delete the widgets of the merge row.
        self.merge_label.destroy()
        del self.merge_label
        self.name.destroy()
        del self.name
        self.left_table_label.destroy()
        del self.left_table_label
        self.left_table.destroy()
        del self.left_table
        self.right_table_label.destroy()
        del self.right_table_label
        self.right_table.destroy()
        del self.right_table
        self.join_types_label.destroy()
        del self.join_types_label
        self.join_types.destroy()
        del self.join_types
        self.left_time_keys_label.destroy()
        del self.left_time_keys_label
        self.left_time_keys_entry.destroy()
        del self.left_time_keys_entry
        self.left_time_key_list.destroy()
        del self.left_time_key_list
        self.right_time_keys_label.destroy()
        del self.right_time_keys_label
        self.right_time_keys_entry.destroy()
        del self.right_time_keys_entry
        self.right_time_key_list.destroy()
        del self.right_time_key_list
        self.left_keys_label.destroy()
        del self.left_keys_label
        self.left_keys_entry.destroy()
        del self.left_keys_entry
        self.left_key_list.destroy()
        del self.left_key_list
        self.right_keys_label.destroy()
        del self.right_keys_label
        self.right_keys_entry.destroy()
        del self.right_keys_entry
        self.right_key_list.destroy()
        del self.right_key_list
        self.delete.destroy()
        del self.delete

    def state(self):
        # Return the state of the row as a dictionary.
        state = {}
        state["type"] = "merge_as_of"
        state["name"] = self.name.get()
        state["left_table"] = self.left_table.get()
        state["right_table"] = self.right_table.get()
        state["join_types"] = self.join_types.curselection()
        state["left_time_key_list"] = {}
        state["left_time_key_list"]["contents"] = self.left_time_key_list.get(0, tk.END)
        state["left_time_key_list"][
            "selection"
        ] = self.left_time_key_list.curselection()
        state["right_time_key_list"] = {}
        state["right_time_key_list"]["contents"] = self.right_time_key_list.get(
            0, tk.END
        )
        state["right_time_key_list"][
            "selection"
        ] = self.right_time_key_list.curselection()
        state["left_key_list"] = {}
        state["left_key_list"]["contents"] = self.left_key_list.get(0, tk.END)
        state["left_key_list"]["selection"] = self.left_key_list.curselection()
        state["right_key_list"] = {}
        state["right_key_list"]["contents"] = self.right_key_list.get(0, tk.END)
        state["right_key_list"]["selection"] = self.right_key_list.curselection()
        return state

    def load_state(self, state):
        # Update the row to match the state provided.
        self.name.insert(0, state["name"])
        self.left_table.insert(0, state["left_table"])
        self.right_table.insert(0, state["right_table"])
        if len(state["join_types"]) != 0:
            self.join_types.selection_set(state["join_types"][0])

        self.left_time_key_list.insert(0, *state["left_time_key_list"]["contents"])
        for index in state["left_time_key_list"]["selection"]:
            self.left_time_key_list.selection_set(index)

        self.right_time_key_list.insert(0, *state["right_time_key_list"]["contents"])
        for index in state["right_time_key_list"]["selection"]:
            self.right_time_key_list.selection_set(index)

        self.left_key_list.insert(0, *state["left_key_list"]["contents"])
        for index in state["left_key_list"]["selection"]:
            self.left_key_list.selection_set(index)

        self.right_key_list.insert(0, *state["right_key_list"]["contents"])
        for index in state["right_key_list"]["selection"]:
            self.right_key_list.selection_set(index)


class FilterVersionNo:
    def __init__(self, master, row_number, app):
        # Create all the widgets of a merge row.
        self.master = master
        self.row_number = row_number
        # Create a label and entry box to name the result of filter
        self.output_label = ttk.Label(self.master, text="Output table")
        self.name = ttk.Entry(self.master)
        self.name.config(width=26)
        # Create entry box to provide the name of the input.
        self.input_label = ttk.Label(self.master, text="Input table")
        self.input = ttk.Entry(self.master)
        self.input.config(width=26)
        # Create a button that deletes the row.
        self.delete = ttk.Button(
            self.master, text="\u274C", command=lambda: app.delete_row(self.row_number)
        )
        self.position()

    def position(self):
        self.output_label.grid(
            row=defaults.query_row_offset + defaults.row_height * self.row_number,
            column=0,
            pady=defaults.query_y_pad,
            padx=defaults.standard_x_pad,
            sticky="ws",
        )
        self.output_label.update()
        self.name.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.names_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.name.update()

        self.input_label.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_label_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
            sticky="ws",
        )
        self.input_label.update()
        self.input.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.input.update()
        self.delete.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.delete_button_internal_row,
            column=defaults.last_column,
            sticky="nw",
        )
        self.delete.update()

    def empty(self):
        # Delete the widgets of the merge row.
        self.output_label.destroy()
        del self.output_label
        self.name.destroy()
        del self.name
        self.input_label.destroy()
        del self.input_label
        self.input.destroy()
        del self.input
        self.delete.destroy()
        del self.delete

    def state(self):
        # Return the state of the row as a dictionary.
        state = {}
        state["type"] = "filter_version_no"
        state["name"] = self.name.get()
        state["input"] = self.input.get()
        return state

    def load_state(self, state):
        # Update the row to match the state provided.
        self.name.insert(0, state["name"])
        self.input.insert(0, state["input"])


class Merge:
    def __init__(self, master, row_number, app):
        # Create all the widgets of a merge row.
        self.master = master
        self.row_number = row_number
        # Create a label and entry box to name the result of the merge
        self.merge_label = ttk.Label(self.master, text="Merge name")
        self.name = ttk.Entry(self.master)
        self.name.config(width=26)
        # Create entry box to provide the name of the left result to merge.
        self.left_table_label = ttk.Label(self.master, text="Left table")
        self.left_table = ttk.Entry(self.master)
        self.left_table.config(width=26)
        # Create an entry box to provide the name of the right result to merge.
        self.right_table_label = ttk.Label(self.master, text="Right table")
        self.right_table = ttk.Entry(self.master)
        self.right_table.config(width=26)
        # Create a list to select the merge type from.
        self.join_types_label = ttk.Label(self.master, text="Select join type")
        self.join_types = tk.Listbox(self.master, exportselection=False, width=28)
        for item in defaults.join_type:
            self.join_types.insert(tk.END, item)
        # Create a button that deletes the row.
        self.delete = ttk.Button(
            self.master, text="\u274C", command=lambda: app.delete_row(self.row_number)
        )
        # Create a entry box and list to provide the keys to the left result.
        self.left_keys_label = ttk.Label(self.master, text="Left keys")
        self.left_keys_entry = ttk.Entry(self.master)
        self.left_keys_entry.bind("<Return>", self.add_to_list_left)
        self.left_key_list = tk.Listbox(
            self.master, selectmode=tk.MULTIPLE, exportselection=False, height=8
        )
        # Create a entry box and list to provide the keys to the right result.
        self.right_keys_label = ttk.Label(self.master, text="Right keys")
        self.right_keys_entry = ttk.Entry(self.master)
        self.right_keys_entry.bind("<Return>", self.add_to_list_right)
        self.right_key_list = tk.Listbox(
            self.master, selectmode=tk.MULTIPLE, exportselection=False, height=8
        )
        # Position all the widgets.
        self.position()

    def position(self):
        self.merge_label.grid(
            row=defaults.query_row_offset + defaults.row_height * self.row_number,
            column=0,
            pady=defaults.query_y_pad,
            padx=defaults.standard_x_pad,
            sticky="ws",
        )
        self.merge_label.update()
        self.name.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.names_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.name.update()
        self.left_table_label.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_label_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.left_table_label.update()
        self.left_table.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.start_time_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.left_table.update()
        self.right_table_label.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.end_time_label_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.right_table_label.update()
        self.right_table.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.end_time_internal_row,
            column=0,
            padx=defaults.standard_x_pad,
        )
        self.right_table.update()
        self.join_types_label.grid(
            row=defaults.query_row_offset + defaults.row_height * self.row_number,
            column=1,
            pady=defaults.query_y_pad,
            sticky="sw",
            padx=defaults.standard_x_pad,
        )
        self.join_types_label.update()
        self.join_types.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.table_list_internal_row,
            column=1,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.join_types.update()
        self.delete.grid(
            row=defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.delete_button_internal_row,
            column=defaults.last_column,
            sticky="nw",
        )
        self.delete.update()

        label_row = defaults.query_row_offset + defaults.row_height * self.row_number
        label_sticky = "w"
        entry_row = (
                defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.names_internal_row
        )
        custom_list_row = (
                defaults.query_row_offset
                + defaults.row_height * self.row_number
                + defaults.internal_filter_row
        )

        left_keys_col = (
                self.join_types.grid_info()["column"] + defaults.list_column_span
        )
        self.left_keys_label.grid(
            row=label_row,
            column=left_keys_col,
            sticky=label_sticky,
            padx=defaults.standard_x_pad,
        )
        self.left_keys_entry.grid(
            row=entry_row,
            column=left_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.left_key_list.grid(
            row=custom_list_row,
            column=left_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )

        right_keys_col = (
                self.left_key_list.grid_info()["column"] + defaults.list_column_span
        )
        self.right_keys_label.grid(
            row=label_row,
            column=right_keys_col,
            sticky=label_sticky,
            padx=defaults.standard_x_pad,
        )
        self.right_keys_entry.grid(
            row=entry_row,
            column=right_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )
        self.right_key_list.grid(
            row=custom_list_row,
            column=right_keys_col,
            rowspan=defaults.list_row_span,
            columnspan=defaults.list_column_span,
            sticky="nw",
            padx=defaults.standard_x_pad,
        )

    def add_to_list_left(self, evt):
        # Add key from entry box to list.
        self.left_key_list.insert(tk.END, evt.widget.get())
        evt.widget.delete(0, "end")

    def add_to_list_right(self, evt):
        # Add key from entry box to list.
        self.right_key_list.insert(tk.END, evt.widget.get())
        evt.widget.delete(0, "end")

    def empty(self):
        # Delete the widgets of the merge row.
        self.merge_label.destroy()
        del self.merge_label
        self.name.destroy()
        del self.name
        self.left_table_label.destroy()
        del self.left_table_label
        self.left_table.destroy()
        del self.left_table
        self.right_table_label.destroy()
        del self.right_table_label
        self.right_table.destroy()
        del self.right_table
        self.join_types_label.destroy()
        del self.join_types_label
        self.join_types.destroy()
        del self.join_types
        self.left_keys_label.destroy()
        del self.left_keys_label
        self.left_keys_entry.destroy()
        del self.left_keys_entry
        self.left_key_list.destroy()
        del self.left_key_list
        self.right_keys_label.destroy()
        del self.right_keys_label
        self.right_keys_entry.destroy()
        del self.right_keys_entry
        self.right_key_list.destroy()
        del self.right_key_list
        self.delete.destroy()
        del self.delete

    def state(self):
        # Return the state of the row as a dictionary.
        state = {}
        state["type"] = "merge"
        state["name"] = self.name.get()
        state["left_table"] = self.left_table.get()
        state["right_table"] = self.right_table.get()
        state["join_types"] = self.join_types.curselection()
        state["left_key_list"] = {}
        state["left_key_list"]["contents"] = self.left_key_list.get(0, tk.END)
        state["left_key_list"]["selection"] = self.left_key_list.curselection()
        state["right_key_list"] = {}
        state["right_key_list"]["contents"] = self.right_key_list.get(0, tk.END)
        state["right_key_list"]["selection"] = self.right_key_list.curselection()
        return state

    def load_state(self, state):
        # Update the row to match the state provided.
        self.name.insert(0, state["name"])
        self.left_table.insert(0, state["left_table"])
        self.right_table.insert(0, state["right_table"])
        if len(state["join_types"]) != 0:
            self.join_types.selection_set(state["join_types"][0])

        self.left_key_list.insert(0, *state["left_key_list"]["contents"])
        for index in state["left_key_list"]["selection"]:
            self.left_key_list.selection_set(index)

        self.right_key_list.insert(0, *state["right_key_list"]["contents"])
        for index in state["right_key_list"]["selection"]:
            self.right_key_list.selection_set(index)
