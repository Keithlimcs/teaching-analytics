-- Database Schema for Teaching Organization Analytics

-- Clients Table
CREATE TABLE clients (
    client_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    industry TEXT,
    size TEXT,  -- Small, Medium, Large, Enterprise
    region TEXT,
    contact_person TEXT,
    email TEXT,
    phone TEXT,
    first_engagement_date TEXT,  -- Date of first engagement
    last_engagement_date TEXT,   -- Date of most recent engagement
    total_spend REAL DEFAULT 0,  -- Total amount spent by client
    notes TEXT
);

-- Programs Table
CREATE TABLE programs (
    program_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,  -- e.g., Leadership, Technical, Soft Skills
    delivery_mode TEXT,  -- In-Person, Virtual, Hybrid
    duration INTEGER,    -- Duration in hours
    base_price REAL,     -- Base price per participant
    min_participants INTEGER,
    max_participants INTEGER,
    trainer_cost_per_session REAL,  -- Cost of trainer per session
    materials_cost_per_participant REAL,  -- Cost of materials per participant
    active INTEGER DEFAULT 1,  -- Boolean (0/1) to indicate if program is active
    creation_date TEXT,
    last_updated TEXT
);

-- Enrollments Table (tracks program runs and participants)
CREATE TABLE enrollments (
    enrollment_id INTEGER PRIMARY KEY,
    program_id INTEGER,
    client_id INTEGER,
    start_date TEXT,
    end_date TEXT,
    location TEXT,
    delivery_mode TEXT,  -- In-Person, Virtual, Hybrid (may differ from program default)
    num_participants INTEGER,
    revenue REAL,        -- Total revenue from this enrollment
    trainer_cost REAL,   -- Total trainer cost for this enrollment
    logistics_cost REAL, -- Travel, accommodation, etc.
    venue_cost REAL,     -- Cost of venue
    utilities_cost REAL, -- Other utilities and miscellaneous costs
    materials_cost REAL, -- Total materials cost
    status TEXT,         -- Scheduled, Completed, Cancelled
    feedback_score REAL, -- Average feedback score (if available)
    notes TEXT,
    FOREIGN KEY (program_id) REFERENCES programs (program_id),
    FOREIGN KEY (client_id) REFERENCES clients (client_id)
);

-- Opportunities Table (sales pipeline)
CREATE TABLE opportunities (
    opportunity_id INTEGER PRIMARY KEY,
    client_id INTEGER,
    program_id INTEGER,
    potential_revenue REAL,
    estimated_participants INTEGER,
    stage TEXT,  -- Lead, Prospect, Proposal, Negotiation, Closed Won, Closed Lost
    probability REAL,  -- Probability of closing (0-100%)
    expected_close_date TEXT,
    actual_close_date TEXT,
    created_date TEXT,
    last_updated TEXT,
    owner TEXT,  -- Sales person responsible
    notes TEXT,
    FOREIGN KEY (client_id) REFERENCES clients (client_id),
    FOREIGN KEY (program_id) REFERENCES programs (program_id)
);

-- Views for Analysis

-- Profitability View
CREATE VIEW program_profitability AS
SELECT 
    e.enrollment_id,
    p.name AS program_name,
    c.name AS client_name,
    e.start_date,
    e.revenue,
    e.trainer_cost,
    e.logistics_cost,
    e.venue_cost,
    e.utilities_cost,
    e.materials_cost,
    (e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) AS profit,
    CASE 
        WHEN e.revenue > 0 
        THEN ((e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / e.revenue) * 100 
        ELSE 0 
    END AS profit_margin
FROM 
    enrollments e
JOIN 
    programs p ON e.program_id = p.program_id
JOIN 
    clients c ON e.client_id = c.client_id
WHERE 
    e.status = 'Completed';

-- Client Spending View
CREATE VIEW client_spending AS
SELECT 
    c.client_id,
    c.name,
    c.industry,
    c.size,
    c.region,
    COUNT(e.enrollment_id) AS total_enrollments,
    SUM(e.revenue) AS total_spend,
    MAX(e.start_date) AS last_enrollment_date
FROM 
    clients c
LEFT JOIN 
    enrollments e ON c.client_id = e.client_id
GROUP BY 
    c.client_id;

-- Program Popularity View
CREATE VIEW program_popularity AS
SELECT 
    p.program_id,
    p.name,
    p.category,
    p.delivery_mode,
    COUNT(e.enrollment_id) AS total_runs,
    SUM(e.num_participants) AS total_participants,
    SUM(e.revenue) AS total_revenue,
    AVG(e.feedback_score) AS avg_feedback
FROM 
    programs p
LEFT JOIN 
    enrollments e ON p.program_id = e.program_id
GROUP BY 
    p.program_id;

-- Sales Pipeline View
CREATE VIEW sales_pipeline AS
SELECT 
    o.stage,
    COUNT(o.opportunity_id) AS num_opportunities,
    SUM(o.potential_revenue) AS potential_revenue,
    SUM(o.potential_revenue * (o.probability / 100)) AS weighted_revenue
FROM 
    opportunities o
WHERE 
    o.stage NOT IN ('Closed Won', 'Closed Lost')
GROUP BY 
    o.stage
ORDER BY 
    CASE 
        WHEN o.stage = 'Lead' THEN 1
        WHEN o.stage = 'Prospect' THEN 2
        WHEN o.stage = 'Proposal' THEN 3
        WHEN o.stage = 'Negotiation' THEN 4
        ELSE 5
    END;
