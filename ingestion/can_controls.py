def validate_can_rows(df, config):
    rules = config["quality_rules"]

    timestamp_index_name = config["source_columns"]["timestamp_index"]
    timestamp_output_col = config["source_columns"]["timestamp_us"]

    aid_col = config["source_columns"]["arbitration_id"]
    dlc_col = config["source_columns"]["dlc"]
    data_col = config["source_columns"]["data"]
    label_col = config["source_columns"]["label"]

    df_check = df.copy()

    if timestamp_output_col not in df_check.columns:
        df_check = df_check.reset_index()

        if timestamp_index_name in df_check.columns:
            df_check = df_check.rename(columns={timestamp_index_name: timestamp_output_col})
        elif "index" in df_check.columns:
            df_check = df_check.rename(columns={"index": timestamp_output_col})

    required_columns = [
        timestamp_output_col,
        aid_col,
        dlc_col,
        data_col,
        label_col,
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df_check.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Colonnes manquantes : {missing_columns}. "
            f"Colonnes disponibles : {df_check.columns.tolist()}"
        )

    valid_mask = (
        df_check[timestamp_output_col].notna()
        & df_check[aid_col].between(
            rules["arbitration_id_min"],
            rules["arbitration_id_max"]
        )
        & df_check[dlc_col].between(
            rules["dlc_min"],
            rules["dlc_max"]
        )
        & df_check[label_col].isin(rules["accepted_labels"])
        & df_check[data_col].notna()
    )

    valid_df = df_check[valid_mask].copy()
    rejected_df = df_check[~valid_mask].copy()

    return valid_df, rejected_df