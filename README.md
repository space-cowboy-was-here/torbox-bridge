# TorBox Bridge

## Overview
**TorBox Bridge** is a unified proxy application that connects your favorite media automation tools (like **Sonarr**, **Radarr**, or **Readarr**) to the **TorBox** cloud download service. 

It acts as a "bridge" by emulating two popular download client APIs:
1. **SABnzbd** (for Usenet downloads)
2. **qBittorrent** (for Torrent downloads)

This allows you to manage both Usenet (NZB) and Torrent (Magnet/File) downloads through a single application, seamlessly offloading the heavy lifting to TorBox's cloud servers while keeping your local automation happy.

## Features
- **Unified Interface**: Use one bridge for both Usenet and Torrents.
- **SABnzbd Emulation**: Full support for adding NZBs, queue management, and history (compatible with Sonarr/Radarr Usenet clients).
- **qBittorrent Emulation**: Support for adding Torrents/Magnets, category management, and progress tracking (compatible with Sonarr/Radarr Torrent clients).
- **Local Download Handling**: Automatically downloads completed files from TorBox to your local storage via Aria2 (built-in).
- **Smart Queue**: Manages concurrency and speed limits to maximize your connection.
- **Web UI**: A clean, dark-mode web interface to monitor active downloads, view logs, and configure settings.
- **Secure Setup**: First-run setup wizard ensures your credentials are stored securely in a local database, not environment variables.

## Project Structure
```
torbox-bridge
├── app
│   ├── templates       # Web UI HTML templates
│   ├── main.py         # Main application logic (Flask)
│   └── ...
├── docker-compose.yml  # Docker deployment config
├── Dockerfile          # Container definition
└── requirements.txt    # Python dependencies
```

## Getting Started

### Prerequisites
- Docker and Docker Compose installed on your machine.

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/torbox-bridge.git
   cd torbox-bridge
   ```

2. **Start the application:**
   ```bash
   docker-compose up -d
   ```
   *By default, the bridge runs on port `5051`.*

3. **Initial Setup:**
   - Open your web browser and navigate to `http://localhost:5051`.
   - You will be greeted with a **Create Account** screen. Create your admin username and password.
   - Once logged in, you will be redirected to the **Settings** page.
   - Enter your **TorBox API Key** (found in your TorBox dashboard) and click Save.

### Client Configuration

#### Connecting Sonarr/Radarr (Usenet)
Configure this as a **SABnzbd** download client.
- **Host**: `Your-Bridge-IP`
- **Port**: `5051`
- **API Key**: `torbox123` (Default, or change in Bridge Settings)
- **SSL**: Off (unless configured via reverse proxy)

#### Connecting Sonarr/Radarr (Torrents)
Configure this as a **qBittorrent** download client.
- **Host**: `Your-Bridge-IP`
- **Port**: `5051`
- **Username**: *The username you created in step 3*
- **Password**: *The password you created in step 3*
- **Aria2**: No (use Standard qBittorrent version 4.1+)

## Usage
- **Web Interface**: `http://localhost:5051` - View queue, history, logs, and settings.
- **Auto-Download**: The bridge will automatically poll TorBox for status updates. Once a file is "Cached" or "Downloaded" on TorBox, the bridge downloads it to your `./downloads` folder (mapped in Docker).

## Development
To run locally without Docker for development:
1. Install dependencies: `pip install -r requirements.txt`
2. Run the app: `python app/main.py`
3. The app will create a `config` folder in the current directory for its database.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.