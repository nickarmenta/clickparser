import pandas as pd
import logging
import os
import glob


def process_folder(input_folder=None):
    # Set up logging configuration
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("contact_ingester.log"), logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    try:
        # Find all CSV files in the input folder
        csv_pattern = os.path.join(input_folder, "*.csv")
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            logger.warning(f"No CSV files found in {input_folder}")
            return
            
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Process each CSV file
        for input_file in csv_files:
            logger.info(f"\nProcessing file: {input_file}")
            read_contact_file(input_file)
            
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing folder: {e}", exc_info=True)
        return None


def read_contact_file(input_file=None):
    # Set up logging configuration
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("contact_ingester.log"), logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    try:
        # Get the directory path and base filename from input_file
        input_dir = os.path.dirname(input_file)
        base_filename = os.path.basename(input_file)
        xlsx_filename = os.path.splitext(base_filename)[0] + '.xlsx'
        
        # Combine input directory with new xlsx filename
        output_file = os.path.join(input_dir, xlsx_filename)
        logger.info(f"Output will be saved to {output_file}")
        
        logger.info(f"Attempting to read file from {input_file}")
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(input_file)
        initial_total = len(df)
        logger.info(f"Initial row count: {initial_total}")

        # Check for NA values before any processing
        na_counts = df["Email address"].isna().sum()
        logger.info(f"Rows with NA in Email address: {na_counts}")
        blank_counts = (df["Email address"] == "").sum()
        logger.info(f"Rows with blank Email address: {blank_counts}")

        # Remove specified columns
        columns_to_remove = [
            "Email status",
            "Email permission status",
            "Email update source",
            "Confirmed Opt-Out Date",
            "Confirmed Opt-Out Source",
            "Confirmed Opt-Out Reason",
            "Phone - home",
            "Phone - mobile",
            "Phone - other",
            "Phone - work",
            "Street address line 1 - Home",
            "Country - Home",
            "Street address line 1 - Other",
            "City - Other",
            "State/Province - Other",
            "Zip/Postal Code - Other",
            "Country - Other",
            "Street address line 1 - Work",
            "City - Work",
            "State/Province - Work",
            "Zip/Postal Code - Work",
            "Country - Work",
            "Supplier",
            "Customer Contact",
            "Custom Field 1",
            "Custom Field 2",
            "Custom Field 3",
            "Custom Field 4",
            "Tags",
            "Source Name",
            "Updated At",
            "Created At",
            "Status",
            "Email Lists",
        ]
        df = df.drop(columns=columns_to_remove, errors="ignore")
        after_columns = len(df)
        logger.info(f"Row count after column removal: {after_columns}")

        # Define common personal email domains to filter out
        personal_domains = {
            'gmail.com',
            'yahoo.com',
            'hotmail.com',
            'outlook.com',
            'icloud.com',
            'aol.com',
            'live.com',
            'msn.com',
            'me.com',
            'mail.com'
        }

        # Remove rows with personal email domains that don't have a company name
        initial_rows = len(df)
        df['Email_Domain'] = df['Email address'].str.split('@').str[1]
        personal_emails_mask = (
            (df['Email_Domain'].isin(personal_domains)) & 
            (df['Company'].isna() | (df['Company'] == ''))
        )
        
        if personal_emails_mask.any():
            removed_count = personal_emails_mask.sum()
            logger.info(f"Removing {removed_count} rows with personal email domains and no company name")
            logger.debug("Examples of removed personal emails:")
            logger.debug(df[personal_emails_mask][['Email address', 'Company']].head())
            
            df = df[~personal_emails_mask]
            
        # Drop the temporary Email_Domain column
        df = df.drop(columns=['Email_Domain'])
        
        # Now proceed with filling empty Company values
        logger.info("Filling remaining empty Company values with email domains")
        company_nulls = df["Company"].isna().sum()
        logger.info(f"Empty Company values being filled: {company_nulls}")

        df["Company"] = df.apply(
            lambda row: (
                row["Company"]
                if pd.notna(row["Company"]) and row["Company"] != ""
                else (
                    row["Email address"].split("@")[1].split(".")[0].title()
                    if pd.notna(row["Email address"])
                    else row["Company"]
                )
            ),
            axis=1,
        )

        # Check duplicates before removal
        click_column = next(
            (col for col in df.columns if col.lower() == "clicked at"), None
        )

        duplicate_mask = df.duplicated(
            subset=["Email address", click_column], keep=False
        )
        duplicate_count = duplicate_mask.sum()
        logger.info(f"Found {duplicate_count} rows that are part of duplicate sets")

        # Remove duplicates
        df = df[~duplicate_mask]
        after_duplicates = len(df)
        df = df.drop(columns="Clicked At", errors="ignore")
        logger.info(f"Row count after duplicate removal: {after_duplicates}")
                # Check duplicates before removal
        click_column = next(
            (col for col in df.columns if col.lower() == "clicked link address"), None
        )
        duplicate_mask = df.duplicated(
            subset=["Email address", click_column], keep="first"
        )
        duplicate_count = duplicate_mask.sum()
        logger.info(f"Found {duplicate_count} rows that are part of duplicate sets")

        # Remove duplicates
        df = df[~duplicate_mask]
        after_duplicates = len(df)
        logger.info(f"Row count after duplicate removal: {after_duplicates}")

        # Convert to strings and handle NaN
        df = df.astype(str)
        df = df.replace("nan", "")
        final_count = len(df)

        # Summary of changes
        logger.info("\nSummary of row counts:")
        logger.info(f"Initial rows: {initial_total}")
        logger.info(f"Final rows: {final_count}")
        logger.info(f"Total rows removed: {initial_total - final_count}")

        # Before saving to Excel, reorder columns to move Owner to first position
        logger.info("Reordering columns to move Owner to first position")
        # Get list of all columns
        cols = df.columns.tolist()
        # Remove Owner from current position
        if 'Owner' in cols:
            cols.remove('Owner')
        else:
            df['Owner'] = ''

        # Create new Task column with empty strings
        df['Task'] = ''

        # Define desired column order
        desired_order = [
            'Owner',
            'Task',
            'Company',
            'Email address',
            'Clicked Link Address'
        ]
        
        # Get remaining columns that aren't in the desired order
        remaining_cols = [col for col in df.columns if col not in desired_order]
        
        # Combine desired order with remaining columns
        final_order = desired_order + remaining_cols
        
        # Reorder DataFrame columns
        df = df[final_order]
        
        # Save DataFrame to Excel
        logger.info(f"Saving processed DataFrame to {output_file}")
        df.to_excel(output_file, index=False, engine='openpyxl')
        logger.info("File saved successfully!")

        return df

    except FileNotFoundError:
        logger.error(f"File not found at {input_file}")
        return None
    except ModuleNotFoundError:
        logger.error("Please install openpyxl: pip install openpyxl")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        return None
