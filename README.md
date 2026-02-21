# Credit Card Reconciliation System

Automated system for reconciling credit card statements with email receipts, featuring deep email analysis, smart matching, and Google Drive integration.

## 🎯 Overview

This system automatically:
1. **Scans your Gmail inbox** for receipt/invoice emails (INBOX, Spam, All Mail)
2. **Categorizes emails** into useful (with receipts) and useless (no receipts)
3. **Matches receipts** to transactions on 3 credit card statements
4. **Creates Excel reports** with detailed reconciliation data
5. **Uploads everything** to organized Google Drive folders
6. **Logs all operations** for complete audit trail

## ✨ Key Features

- ✅ **100% Automated** - No manual intervention required
- ✅ **Deep Email Checking** - Scans subject, body, and attachments
- ✅ **Smart Matching** - Fuzzy merchant name matching with 85% threshold
- ✅ **Comprehensive Logging** - Tracks matched, unmatched, and useless emails
- ✅ **Excel Reports** - Detailed reconciliation with amount differences
- ✅ **Google Drive Integration** - Organized folder structure with auto-upload
- ✅ **Complete Folder Upload** - Uploads entire folders, not just individual files
- ✅ **Multi-Card Support** - Handles Meriwest (PDF), Amex (Excel), Chase (Excel)
- ✅ **Image Support** - Processes PDF and image receipts (jpg, png, gif)
- ✅ **Sender Folder Upload** - Automatically uploads all sender email folders

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install pandas fuzzywuzzy python-levenshtein google-api-python-client google-auth-oauthlib google-auth pymupdf openpyxl python-dotenv python-dateutil
```

### 2. Configure Environment
Edit `.env` file:
```env
EMAIL_ADDRESS=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
DRIVE_FOLDER_ID=your_drive_folder_id
```

### 3. Add Statement Files
Place these files in `statements/` folder:
- `Meriwest_Credit_Card_Statement.pdf`
- `Amex_Credit_Card_Statement.xlsx`
- `Chase_Credit_Card_Statement.xlsx`

### 4. Test Setup
```bash
python test_setup.py
```

### 5. Run Reconciliation
```bash
python main.py
```

## 📁 Output Structure

```
statements/
├── [Sender_Name_Folders]/         # Email sender folders with receipts
├── Meriwest_Reconciled/
│   ├── Meriwest_Reconciliation.xlsx
│   └── [Labeled receipts: Meriwest_001.pdf, etc.]
├── Amex_Reconciled/
│   ├── Amex_Reconciliation.xlsx
│   └── [Labeled receipts: Amex_001.pdf, etc.]
└── Chase_Reconciled/
    ├── Chase_Reconciliation.xlsx
    └── [Labeled receipts: Chase_001.pdf, etc.]

temp/
├── useless_emails_log.xlsx       # Non-receipt emails
├── matched_emails_log.xlsx       # Receipt emails
└── Unmatched_Receipts/
    ├── Unmatched_Receipts_Log.xlsx
    └── [Unmatched receipt files]

Google Drive (DRIVE_FOLDER_ID)/
├── Meriwest_Reconciled/          # Complete folder uploaded
├── Amex_Reconciled/              # Complete folder uploaded
├── Chase_Reconciled/             # Complete folder uploaded
├── Unmatched_Receipts/           # Complete folder uploaded
└── Sender_Email_Folders/         # All sender folders uploaded
    ├── Floor_and_Decor/
    ├── Grainger/
    └── [All other sender folders]
```

## 📊 Excel Reports

### Reconciliation Report
Each card gets a detailed Excel report with:
- Receipt Label (e.g., Amex_001)
- Transaction Date, Description, Amount
- Receipt Merchant, Amount
- Amount Difference
- File Paths

### Email Logs
- **Matched Emails**: All receipt emails with sender, subject, attachments
- **Useless Emails**: Non-receipt emails with reason for classification

## 🔍 How It Works

### Step 1: Email Fetching
- Connects to Gmail via IMAP
- Scans last 500 emails from INBOX, Spam, All Mail
- Identifies receipts using keywords: receipt, invoice, bill, payment, order, confirmation
- Saves attachments to sender-specific folders

### Step 2: Receipt Parsing
- Extracts data from PDF receipts (merchant, amount, date)
- Includes image files as receipts
- Creates Receipt objects for matching

### Step 3: Statement Reconciliation
For each credit card:
1. Parse transactions from statement file
2. Match receipts using fuzzy matching (85% threshold, ±$1 tolerance)
3. Assign unique labels (CardName_###)
4. Create Excel reconciliation report
5. Upload to Google Drive

### Step 4: Unmatched Collection
- Identifies receipts not matched to any statement
- Creates Excel log
- Uploads complete folder to separate Drive location

### Step 5: Sender Folder Upload (New!)
- Uploads all sender email folders from statements directory
- Creates organized structure in Google Drive
- Maintains folder hierarchy with all files

## ⚙️ Configuration

### Environment Variables (.env)
```env
EMAIL_ADDRESS=mohammadqasimkamran@gmail.com
EMAIL_PASSWORD=emox plcu bbgg cjfe
IMAP_SERVER=imap.gmail.com
DRIVE_FOLDER_ID=1YPCecSnpJ1gTvvtENZ2ElvAtipLNB3ry
STATEMENTS_DIR=statements/
TEMP_DIR=temp/
MATCH_THRESHOLD=85
```

### Google Drive Setup
1. Create Google Cloud project
2. Enable Google Drive API
3. Download `credentials.json`
4. First run opens browser for OAuth
5. Token saved to `token.pickle`

## 🎯 Customization

### Adjust Email Limit
```python
# In src/reconciler.py
client.fetch_and_save_emails(limit_per_folder=500)  # Change 500
```

### Modify Match Threshold
```env
# In .env
MATCH_THRESHOLD=85  # Lower = more matches
```

### Add Keywords
```python
# In src/email_client.py
receipt_keywords = [
    "receipt", "invoice", "bill",
    # Add your keywords here
]
```

## 📚 Documentation

- **[QUICK_START.md](QUICK_START.md)** - Get started in 3 steps
- **[RECONCILIATION_GUIDE.md](RECONCILIATION_GUIDE.md)** - Complete English guide
- **[URDU_GUIDE.md](URDU_GUIDE.md)** - Complete Urdu guide (اردو گائیڈ)
- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Technical details

## 🔧 Project Structure

```
gmail_auto/
├── main.py                    # Entry point
├── test_setup.py             # Setup verification
├── .env                      # Configuration
├── credentials.json          # Google OAuth credentials
├── token.pickle             # Google OAuth token
├── src/
│   ├── config.py            # Configuration management
│   ├── email_client.py      # Email fetching & categorization
│   ├── pdf_parser.py        # PDF receipt parsing
│   ├── matcher.py           # Receipt-transaction matching
│   ├── reconciler.py        # Main reconciliation logic
│   ├── drive_uploader.py    # Google Drive integration
│   └── models.py            # Data models
├── statements/              # Credit card statements
├── temp/                    # Temporary files & logs
└── receipts/               # Additional receipts
```

## 🐛 Troubleshooting

### No Receipts Found
- Check keywords match your email content
- Verify emails have attachments
- Review `matched_emails_log.xlsx`

### Poor Matching Results
- Lower `MATCH_THRESHOLD` (try 75 or 70)
- Check merchant name variations
- Review `Unmatched_Receipts_Log.xlsx`

### Drive Upload Fails
- Verify `DRIVE_FOLDER_ID`
- Check `credentials.json` exists
- Re-authenticate (delete `token.pickle`)

### Email Connection Fails
- Verify email/password in `.env`
- Use Gmail App Password (not regular password)
- Enable IMAP in Gmail settings

## 📝 Logging

All operations logged to:
- **Console**: Real-time progress with ✓/✗ indicators
- **reconciliation.log**: Detailed operation log with timestamps

## 🔒 Security

- Email credentials stored in `.env` (add to .gitignore)
- Google credentials in `credentials.json` (add to .gitignore)
- OAuth token in `token.pickle` (add to .gitignore)
- Never commit sensitive files to version control

## 📊 Success Metrics

After completion, you'll have:
- ✅ 3 reconciled folders (one per card)
- ✅ Excel reports with matched transactions
- ✅ Labeled receipts organized by card
- ✅ Unmatched receipts in separate folder
- ✅ Useless emails logged with reasons
- ✅ Everything uploaded to Google Drive

## 🎉 Example Output

```
================================================================================
STARTING COMPREHENSIVE CREDIT CARD RECONCILIATION
================================================================================

[STEP 1] Fetching and categorizing emails from inbox...
✓ Processed emails - Matched: 45, Useless: 123

[STEP 2] Parsing receipts from email attachments...
✓ Found 45 receipt files

[STEP 3.1] Reconciling Meriwest credit card...
  Parsed 35 transactions from PDF
  ✓ Matched 12 receipts to Meriwest
  ✓ Created reconciliation Excel: Meriwest_Reconciliation.xlsx
  ✓ Uploaded to Google Drive: Meriwest_Reconciliation.xlsx

[STEP 3.2] Reconciling Amex credit card...
  Parsed 52 transactions from Excel
  ✓ Matched 18 receipts to Amex
  ✓ Created reconciliation Excel: Amex_Reconciliation.xlsx
  ✓ Uploaded to Google Drive: Amex_Reconciliation.xlsx

[STEP 3.3] Reconciling Chase credit card...
  Parsed 41 transactions from Excel
  ✓ Matched 10 receipts to Chase
  ✓ Created reconciliation Excel: Chase_Reconciliation.xlsx
  ✓ Uploaded to Google Drive: Chase_Reconciliation.xlsx

[STEP 4] Collecting unmatched receipts...
  Found 5 unmatched receipts
  ✓ Created unmatched receipts log: Unmatched_Receipts_Log.xlsx
  Uploading Unmatched_Receipts folder to Google Drive...
  ✓ Uploaded complete folder to Google Drive: Unmatched_Receipts/

[STEP 5] Uploading sender folders to Google Drive...
  Uploading sender folder: Floor_and_Decor
  Uploading sender folder: Grainger
  Uploading sender folder: Kohler
  ✓ Uploaded 15 sender folders to Google Drive

================================================================================
✓ ALL RECONCILIATION COMPLETED SUCCESSFULLY!
================================================================================
```

## 🤝 Contributing

This is a custom reconciliation system. For modifications:
1. Update keywords in `src/email_client.py`
2. Adjust matching threshold in `.env`
3. Modify statement parsing in `src/reconciler.py`

## 📄 License

Private project for credit card reconciliation.

## 👨‍💻 Author

Developed for automated credit card statement reconciliation with Gmail integration.

---

**Ready to start?** Run `python test_setup.py` then `python main.py`
