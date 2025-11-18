# NinjaOne to Freshservice Asset Sync

![PowerShell](https://img.shields.io/badge/PowerShell-5.1%2B-blue)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

An enterprise-grade PowerShell script to configure and synchronize your NinjaOne devices as assets in Freshservice. This tool is designed for secure, automated, and scheduled execution, making it easy to keep your asset inventory up-to-date.

## Key Features

-   **✨ Interactive Configuration Wizard**: A guided CLI wizard (`-Mode config`) to map NinjaOne device roles (e.g., "Windows Server", "Windows Workstation") to specific Freshservice Asset Types.
-   **🔐 Secure Credential Management**: Avoids storing plain-text secrets. Integrates with the PowerShell `SecretManagement` module, environment variables, or command-line parameters for secure credential handling.
-   **⚡ Efficient Delta Syncing**: After the first run, the script only processes devices that have been updated in NinjaOne since the last successful sync, saving time and API calls.
-   **🔄 Create & Update Logic**: Can be configured to either create new assets or update existing ones, preventing duplicates.
-   **🛡️ Dry Run Mode**: Run the sync process in a simulation mode (`-DryRun`) to see what actions would be taken without making any changes in Freshservice.
-   **🔗 Flexible Field Mapping**: The configuration wizard allows you to map specific NinjaOne data fields (like serial number, OS, or IP address) to your custom fields in Freshservice.
-   **📦 Automatic Product Creation**: If a device model doesn't exist as a "Product" in Freshservice, the script can automatically create it during the sync process.
-   **📞 Webhook Notifications**: Sends summary notifications to Microsoft Teams or Slack upon completion, providing visibility into sync status (success, failure, items processed).
-   **📈 Robust Error Handling & Logging**: Implements API call retries with exponential backoff and maintains detailed daily log files for easy troubleshooting.

## Prerequisites

1.  **Windows PowerShell 5.1** or later.
2.  **API Credentials**:
    -   **NinjaOne**: A Client ID and Client Secret with `monitoring` and `management` scopes.
    -   **Freshservice**: An API Key for a user with permissions to manage assets.
3.  **PowerShell Modules (Recommended)**:
    -   `Microsoft.PowerShell.SecretManagement` for secure credential storage. Install with:
        ```powershell
        Install-Module Microsoft.PowerShell.SecretManagement
        Install-Module Microsoft.PowerShell.SecretStore
        Register-SecretVault -Name LocalStore -ModuleName Microsoft.PowerShell.SecretStore -DefaultVault
        ```
4.  Network connectivity from the executing machine to both NinjaOne and Freshservice APIs.

## Setup & Configuration

### 1. File Structure

Download `Ninja-Fresh-Sync-Script.ps1` and create a `config.json` file in the same directory. Your folder should look like this:

```
/your-script-folder/
├── Ninja-Fresh-Sync-Script.ps1
└── config.json
```

### 2. Create `config.json`

Use the template below for your `config.json` file.

```json
{
  "NinjaRegion": "oc.ninjarmm.com",
  "FreshSubdomain": "your-company",
  "Secrets": {
    "UseSecretManagement": true,
    "NinjaClientId": "NinjaOne-ClientID",
    "NinjaClientSecret": "NinjaOne-ClientSecret",
    "FreshApiKey": "Freshservice-APIKey"
  },
  "Webhook": {
    "Uri": "",
    "Format": "Teams"
  }
}
```

-   **`NinjaRegion`**: Your NinjaOne instance URL (e.g., `oc.ninjarmm.com`, `eu.ninjarmm.com`).
-   **`FreshSubdomain`**: Your Freshservice domain (if your URL is `my-company.freshservice.com`, this value is `my-company`).
-   **`Secrets`**:
    -   `UseSecretManagement`: Set to `true` (recommended) to use the PowerShell Secret Vault, or `false` to use the plaintext values below (not recommended for production).
    -   `NinjaClientId`, `NinjaClientSecret`, `FreshApiKey`: These are the **names** of the secrets in your vault if `UseSecretManagement` is `true`.
-   **`Webhook`**:
    -   `Uri`: The incoming webhook URL for your Teams channel or Slack app.
    -   `Format`: Set to `Teams` or `Slack`.

### 3. Store Your Credentials Securely (Recommended)

Store your API credentials in your PowerShell Secret Vault using the secret names defined in `config.json`.

```powershell
Set-Secret -Name "NinjaOne-ClientID" -Secret "your_ninja_client_id"
Set-Secret -Name "NinjaOne-ClientSecret" -Secret "your_ninja_client_secret"
Set-Secret -Name "Freshservice-APIKey" -Secret "your_freshservice_api_key"
```

### 4. Run the Interactive Configuration

The first step is to generate the `ninja_fs_mapping.json` file. This file tells the script how to map NinjaOne devices to Freshservice assets.

Run the script in `config` mode. You will need to provide your credentials as parameters for this **one-time run** so the script can connect to the APIs.

```powershell
.\Ninja-Fresh-Sync-Script.ps1 -Mode config -NinjaClientId "..." -NinjaClientSecret "..." -FreshApiKey "..."
```

Follow the on-screen prompts to:
1.  Map NinjaOne device roles to Freshservice Asset Types.
2.  Map required and optional Freshservice fields to fields from the NinjaOne device data.
3.  Select a unique identifier (e.g., serial number) for matching devices to assets.
4.  Map device models to Freshservice Products.

This process will create the `ninja_fs_mapping.json` file.

## Usage

Once configured, you can run the script in `sync` mode. The script will use the credentials stored in your Secret Vault.

#### Standard Sync

Creates new assets in Freshservice. Does not update existing ones by default.

```powershell
.\Ninja-Fresh-Sync-Script.ps1 -Mode sync
```

#### Sync with Updates

Creates new assets and updates existing assets if changes are detected in NinjaOne.

```powershell
.\Ninja-Fresh-Sync-Script.ps1 -Mode sync -UpdateExisting
```

#### Dry Run

Simulates the sync process and logs what would happen, but makes no actual changes. Highly recommended for first-time runs.

```powershell
.\Ninja-Fresh-Sync-Script.ps1 -Mode sync -DryRun -UpdateExisting
```

### Automation (Scheduled Task)

You can automate the sync by creating a Scheduled Task in Windows to run the script daily.

-   **Action**: `Start a program`
-   **Program/script**: `powershell.exe`
-   **Add arguments (optional)**: `-ExecutionPolicy Bypass -File "C:\Path\To\Ninja-Fresh-Sync-Script.ps1" -Mode sync -UpdateExisting`

## File Descriptions

-   **`Ninja-Fresh-Sync-Script.ps1`**: The main executable script.
-   **`config.json`**: Contains your core settings like API endpoints and secret names.
-   **`ninja_fs_mapping.json`** (Generated): Stores the detailed mapping rules created by the interactive configuration wizard. **Do not edit manually unless you are an advanced user.**
-   **`state.json`** (Generated): Stores the timestamp of the last successful sync to enable delta-syncing.
-   **`/logs/`** (Generated): Contains daily log files for auditing and troubleshooting.

## Security

-   **No Hardcoded Credentials**: The script is designed to resolve credentials at runtime from a secure source.
-   **Secret Vault First**: The PowerShell SecretManagement module is the recommended and default method for handling secrets.
-   **Environment Variables**: As a fallback, the script can read credentials from environment variables (`NINJAONE_CLIENT_ID`, `NINJAONE_CLIENT_SECRET`, `FRESHSERVICE_API_KEY`).
-   **Avoid Plaintext**: It is strongly recommended to set `UseSecretManagement` to `true` and avoid storing API keys directly in `config.json`.

## License

This project is licensed under the GNU GPLv3 License. See the [LICENSE](LICENSE) file for details.
