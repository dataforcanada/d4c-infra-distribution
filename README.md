## Data Dissemination Strategy High-Level Overview

```mermaid
flowchart TD
    classDef linkNode stroke:#0000EE,color:#0000EE,stroke-width:2px;
    subgraph mirrors [Mirrors & Preservation]
        SourceCoop[Source Cooperative]
        Zenodo[Zenodo]
        InternetArchive[Internet Archive]
    end

    Sources[Open Data Sources]
    Processes[Transformation Pipelines]
    Artifacts[Systems-Ready Data]
    
    subgraph CoreInfra [Data for Canada Infrastructure]
        Portal[Object Storage]
        Metadata[FAIR Data Catalog]
    end
    
    Distribution[Decentralized Distribution]
    Torrent["P2P Technology"]
    
    subgraph Consumers [Consumption]
        Users[Data People & Developers]
        Systems[Automated Systems]
    end

    %% Flow with Animations
    Sources a1@--> Processes
    a1@{animate: true, animation: slow}
    
    Processes a2@--> Artifacts
    a2@{animate: true, animation: slow}
    
    Artifacts a3@--> Portal
    a3@{animate: true, animation: slow}
    
    Portal a4@--> Metadata
    a4@{animate: true, animation: fast}
    
    Metadata a5@--> Distribution
    a5@{animate: true, animation: fast}

    %% Distribution Logic
    Distribution a7@-.->|Primary| mirrors
    a7@{animate: true, animation: slow}
    
    Distribution a8@-.->|Resiliency| Torrent
    a8@{animate: true, animation: slow}

    %% Mirror Connections
    mirrors a12@--> Consumers
    a12@{animate: true, animation: slow}
    
    mirrors a9@-.->|Pooled| Torrent
    a9@{animate: true, animation: fast}

    %% Torrent Connections
    Torrent a10@--> Consumers
    a10@{animate: true, animation: fast}
    
    %% Click Actions
    click Sources "https://www.dataforcanada.org/#high-level-overview" _blank
    click Processes "https://www.dataforcanada.org/docs/processes/" _blank
    click Artifacts "https://www.dataforcanada.org/docs/getting_started/" _blank
    click Metadata "https://stac-utils.github.io/stac-geoparquet/latest/spec/stac-geoparquet-spec/" _blank
    click Zenodo "https://zenodo.org/communities/dataforcanada/" _blank
    click SourceCoop "https://source.coop/dataforcanada/" _blank
    click InternetArchive "https://archive.org/details/@diegoripley/uploads/" _blank

    %% APPLY STYLES TO LINKED NODES
    class Sources,Processes,Artifacts,Metadata,SourceCoop,Zenodo,InternetArchive linkNode
```

## 🏗️ Open Processing Architecture

We believe that true open data requires open production. To ensure the longevity and resilience of Canada's data infrastructure, we treat our data pipelines as **open source software artifacts**. We provide the "blueprints" alongside the data, allowing any user to verify our work or rebuild the dataset from scratch on their own infrastructure.

### The Blueprint Model

Our processing strategy relies on three immutable components to guarantee transparency:

1. **Build Manifests:** Every dataset version is accompanied by a strict manifest. This locks the exact "ingredients" used: the cryptographic hashes of the raw source files, the specific Git commit of the processing code, and the configuration parameters.
2. **Environment Definitions:** Rather than opaque binaries, we publish the exact **Infrastructure as Code (IaC)** definitions (e.g., Dockerfiles). This allows users to inspect the system context (GDAL versions, libraries, and dependencies) and build the environment themselves.
3. **Deterministic Builds:** By combining a *Build Manifest* with our *Environment Definitions*, any user can execute a **deterministic build**. This process guarantees a bit-for-bit identical copy of the official Data for Canada artifact, ensuring that the pipeline is independent of our specific servers.

**Mirrored Source Artifacts:**
Crucially, we do not rely solely on external version control systems like GitHub, which may change or disappear. A complete snapshot of the processing code, environment definitions, and manifests is bundled with every data release. These source artifacts are replicated across **Source Cooperative, Zenodo, the Internet Archive, Data for Canada infrastructure, and the community**, ensuring that the *method* of creation is preserved with the same redundancy as the *result*.

## Dissemination Process

Once data products reach a production-ready state, they enter a dissemination flow designed for permanence and performance:

* **Cloud-Native First:** Priority is given to performant, system-to-system file formats (e.g., Parquet) to enable high-throughput applications without the need for local parsing.
* **Persistent Identification:** Every dataset version is assigned a DOI for citation and immutability.
* **The FAIR Data Catalog:** Global metadata is aggregated into a single, queryable **[FAIR Data Catalog](https://stac-utils.github.io/stac-geoparquet/latest/spec/stac-geoparquet-spec/)**. This catalog acts as the "brain" of the system, tracking all versions and DOIs, and directing users to the optimal source within our multi-tier storage network:
    - **[Source Cooperative](https://source.coop/dataforcanada)** serves as our **primary mirror** for all datasets, including large-scale products like orthoimagery.
    - **[Zenodo](https://zenodo.org/communities/dataforcanada/)** serves as our repository for **long-term academic preservation** and provides a high-speed mirror for European users.
    - **[The Internet Archive](https://archive.org)** is utilized **strategically** for specific datasets to ensure historical redundancy.
    - **[Data for Canada Infrastructure](https://www.dataforcanada.org/infrastructure/)** is utilized **strategically** for specific datasets of high-value.

### Decentralized Distribution

We are piloting a <abbr title="Peer-to-Peer">P2P</abbr> technology, to maximize infrastructure resilience. By leveraging the [P2P HTTP  consumption feature](https://www.bittorrent.org/beps/bep_0019.html), users will be able to download simultaneously from Source Cooperative, Zenodo, Data for Canada infrastructure, and community peers. This ensures high availability without a single point of failure. Current laboratory work is available in the [Decentralized Distribution Labs](https://github.com/dataforcanada/decentralized-distribution-labs).

## Work in the Lab: Smart Nodes

To further democratize access and ensure the persistence of Canada’s open data, we are experimenting with the features defined in previous work done by other organizations.

A Smart Node functions as a "set-it-and-forget-it" volunteer server, an automated library branch for our data infrastructure.

* **Automated Mirroring:** Unlike a standard download, a Smart Node automatically synchronizes with our central **FAIR Data Catalog**. It intelligently fetches new or "at-risk" datasets to ensure they remain available even if the central portal experiences downtime.
* **Volunteer-Powered Resilience:** This model allows partner institutions (ex. universities, research labs) and public volunteers to donate bandwidth and storage. By running a Smart Node, contributors actively protect vital Canadian datasets from being lost or gated.

* **Dynamic Storage Management:** The node software monitors network health to optimize resource usage. Leveraging the  <abbr title="Peer-to-Peer">P2P</abbr> technology's capability for **selective piece mapping**, the node does not need to store the entire catalog. Instead, it identifies specific file indices or "rare" pieces within the metadata and sends granular `REQUEST` messages for only those blocks. This allows a node with limited storage (ex. 500GB) to provide critical redundancy for a much larger archive (ex. 50TB) by surgically targeting only the data that is currently under shared.

We are currently refining the concepts from [smart-node-transmission](https://github.com/academictorrents/smartnode-transmission) to work seamlessly with our catalog, enabling a fully decentralized data mesh for Canadian geospatial information.

```mermaid
graph TD
    %% Node Definitions with custom labels
    Catalog[("FAIR Data Catalog")]
    SmartNode["Volunteer Smart Node<br/>(Limited Storage Optimization)"]
    BTNetwork(["P2P Network<br/>(Massive Data Pool)"])

    %% The Process Flow
    Catalog -->|"1. Syncs metadata"| SmartNode
    
    note["Note: The Node does NOT<br/>download the whole file."]
    SmartNode -.- note

    SmartNode -->|"2. Sends granular REQUESTs for specific pieces only<br/>(e.g., 'Send Piece #804 of Dataset B')"| BTNetwork
    
    BTNetwork -.->|"3. Transfers ONLY the requested blocks"| SmartNode

    %% Optional Styling for visual clarity
    classDef central fill:#e1f5fe,stroke:#0277bd,stroke-width:2px;
    classDef node fill:#fff9c4,stroke:#fbc02d,stroke-width:2px,stroke-dasharray: 5 5;
    classDef network fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    
    class Catalog central;
    class SmartNode node;
    class BTNetwork network;
```

## Infrastructure High-Level Overview

```mermaid
flowchart TD
    %% ---------------------------------------------------------
    %% STYLING
    %% ---------------------------------------------------------
    style Canada_Region fill:#ffe6e6,stroke:#ff0000,stroke-width:2px
    style USA_Region fill:#e6f2ff,stroke:#0066cc,stroke-width:2px
    style Europe_Region fill:#e6ffe6,stroke:#009900,stroke-width:2px
    
    %% Highlight Primary Storage
    style R2 fill:#fffde7,stroke:#fbc02d,stroke-width:4px

    %% ---------------------------------------------------------
    %% REGION: CANADA
    %% ---------------------------------------------------------
    subgraph Canada_Region ["🇨🇦 Physical Location: Canada"]
        direction TB
        NodeTO["Smart Node 01
Location: Toronto, CA
Specs: 50Gbps / 50Gbps, 950GB Flash Storage
Jurisdiction: Canada"]
        
        IA_Van["Internet Archive Mirror
Location: Vancouver, CA
Protocol: HTTP
Jurisdiction: USA"]
    end

    %% ---------------------------------------------------------
    %% REGION: USA
    %% ---------------------------------------------------------
    subgraph USA_Region ["🇺🇸 Physical Location: USA"]
        direction TB
        SourceCoop["Source Cooperative
Location: Oregon, USA
Protocol: HTTP
Jurisdiction: USA"]
        
        R2["☁️ Cloudflare R2
(Primary Object Storage)
Location: Eastern North America
Protocol: HTTP
Jurisdiction: USA"]
        
        IA_SF["The Internet Archive
Location: San Francisco, USA
Protocol: HTTP
Jurisdiction: USA"]
        
        Netcup["VPS 01
Location: Manassas, Virginia, USA
Specs: 2.5Gbps / 2.5Gbps, 512GB Flash Storage
Protocol: HTTP & P2P
Jurisdiction: Germany"]
    end

    %% ---------------------------------------------------------
    %% REGION: EUROPE
    %% ---------------------------------------------------------
    subgraph Europe_Region ["🇪🇺 Physical Location: Europe"]
        direction TB
        subgraph Netherlands ["🇳🇱 Netherlands"]
            NodeAMS["Smart Node 02
Location: Amsterdam, NL
Specs: 50Gbps / 50Gbps, 950GB Flash Storage
Jurisdiction: Netherlands"]
        end
        
        subgraph Switzerland ["🇨🇭 Switzerland"]
            Zenodo["Zenodo
Location: Geneva, CH
(Replicated in Budapest, HU)
Protocol: HTTP
Jurisdiction: Switzerland"]
        end
    end

    %% ---------------------------------------------------------
    %% CONNECTIONS
    %% ---------------------------------------------------------
    
    NodeTO <==>|P2P| NodeAMS
    IA_SF -.->|Internal Replication| IA_Van
    
    NodeTO -.->|HTTP Pull| SourceCoop
    NodeTO ==> R2
    NodeTO -.->|HTTP Pull| Zenodo
    NodeTO -.->|HTTP Pull| IA_SF
    NodeTO -.->|HTTP or P2P| Netcup

```

## Infrastructure & Operating Costs

| Service | Description | CAD | USD | EUR |
| :--- | :--- | :--- | :--- | :--- |
| **CDN** | [CDN - Cloudflare Details](https://www.cloudflare.com/plans/) - WAF, CDN (Amortized Annual) | $30.90 | $22.60 | €19.13 |
| **CDN Services** | [Object Storage - Cloudflare Details](https://www.cloudflare.com/products/r2/) & [Serverless - Cloudflare Details](https://www.cloudflare.com/en-ca/plans/developer-platform/) (Variable) | $32.71 | $23.93 | €20.26 |
| **Smart Node 01** | [Decentralized Distribution - SlashN Services Details](https://ultra.cc/#plan-pricing) - Dedicated <abbr title="Peer-to-Peer">P2P</abbr> client | $28.98 | $21.21 | €17.95 |
| **Smart Node 02** | [Decentralized Distribution - SlashN Services Details](https://ultra.cc/#plan-pricing) - Dedicated <abbr title="Peer-to-Peer">P2P</abbr> client | $28.98 | $21.21 | €17.95 |
| **VPS 01** | [Geospatial Services - Netcup Details](https://www.netcup.com/en/server/root-server) - ARM64 | $14.64 | $10.72 | €9.07 |
| **TOTAL** | **Monthly Run Rate** | **$136.21** | **$99.67** | **€84.36** |

**Note:** Currency conversions are based on rates from February 16, 2026.

## Roadmap: Resilience & Transparency

To support our mission of providing high-performance, analysis-ready data, we are currently developing a suite of public tools to make this distributed ecosystem more **FAIR** (Findable, Accessible, Interoperable, Reusable), **resilient**, and **transparent**.

These planned features are designed to help researchers and automated systems coordinate data access across the various platforms and mirrors we utilize.

### 1. Real-Time Service Status

We are building a comprehensive status dashboard that monitors the availability of the diverse storage locations we rely on, from our own Smart Nodes to external providers like the Internet Archive, Source Cooperative, and Zenodo. Users will be able to verify if a specific mirror is operational before initiating workflows.

### 2. Traffic & Load Optimization Statistics

To foster better cooperation between our systems and downstream users, we plan to expose traffic and connectivity statistics where possible.

This transparency allows automated systems to be "smart" about data retrieval. For example, a system could query these statistics to schedule bandwidth-intensive HTTP downloads during non-peak hours, or adjust behavior based on current connectivity loads during high-traffic periods of the workday. This improves performance for individual users while respecting the bandwidth constraints of the various host providers.

### 3. Community Issue Reporting

We are introducing a streamlined method for users to report access issues across any of the services we aggregate. By allowing the community to flag connectivity drops or data integrity issues quickly, we can identify bottlenecks or outages at specific providers and route users to alternative sources more effectively.
