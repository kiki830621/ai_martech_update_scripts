# AI Marketing Platform Dashboard
# This Shiny app visualizes the customer DNA analysis results
# 
# CONSTRUCTION PRINCIPLES:
# 1. Content Treatment Rule:
#    - Constant elements (panel structure, UI components, interaction patterns, code architecture)
#      must be preserved exactly as in the KitchenMAMA reference app
#    - Variable elements (data sources, terminology, brand elements, data processing)
#      may be adapted to meet client-specific requirements
# 
# 2. Scroll-Free UI Rule:
#    - Avoid unnecessary scrolling in the app interface
#    - Use navigation panels (nav_panel) instead of long scrollable pages
#    - Keep all critical information visible without requiring scrolling
#
# 3. Brand Protection Rule:
#    - Never display company or brand names in user-facing elements
#    - Use generic application names and descriptions
#    - Maintain confidentiality of client identity in all UI components
#
# See app_principles.md for detailed documentation

# Load required packages
library(shiny)
library(bslib)
library(bsicons)
library(dplyr)
library(gridlayout)
library(lubridate)
library(ggplot2)
library(DBI)
library(duckdb)
library(tidyr)
library(plotly)
library(readxl)
library(scales)
library(DT)
library(shinyjs)

# Source global parameters and brand-specific settings
source(file.path("local_scripts", "brand_specific_parameters.R"))
source(file.path("../../../../global_scripts", "03_config", "global_parameters.R"))

# Source modules and utilities
source(file.path("../../../../global_scripts", "10_rshinyapp_modules", "common", "sidebar.R"))
source(file.path("../../../../global_scripts", "10_rshinyapp_modules", "macro", "macro_overview.R"))
source(file.path("../../../../global_scripts", "10_rshinyapp_modules", "micro", "micro_customer.R"))
source(file.path("../../../../global_scripts", "10_rshinyapp_modules", "marketing", "target_profiling.R"))
source(file.path("../../../../global_scripts", "11_rshinyapp_utils", "helpers.R"))
source(file.path("../../../../global_scripts", "11_rshinyapp_utils", "formattime.R"))

# Data source function 
# Provides reactive access to various data sources
data_source_function <- function() {
  # Source the centralized database connection functions
  source(file.path("../../../../global_scripts", "02_db_utils", "100g_dbConnect_from_list.R"))
  source(file.path("../../../../global_scripts", "02_db_utils", "103g_dbDisconnect_all.R"))
  
  # Connect to databases using the standardized connection function
  connections <- reactiveValues(app_data = NULL, processed_data = NULL)
  
  # Initialize connections
  observe({
    tryCatch({
      # Connect to app_data (primary app database)
      connections$app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = TRUE)
      
      # Also connect to processed_data for any additional needed data
      # This follows the data flow principles where processed_data feeds into app_data
      connections$processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = TRUE)
    }, error = function(e) {
      message("Database connection error: ", e$message)
      message("Using fallback data sources instead")
    })
  })
  
  # Check for local RDS files in app_data/ directory
  local_data_available <- reactive({
    dir.exists("app_data") && length(list.files("app_data", pattern = "\\.rds$")) > 0
  })
  
  # Cleanup on session end
  onStop(function() {
    # Use the standardized disconnect function to close all connections
    # This ensures all database connections are properly tracked and closed
    dbDisconnect_all(verbose = FALSE)
  })
  
  # Helper function to safely read RDS files with fallback
  safe_read_rds <- function(file_path, fallback = NULL) {
    tryCatch({
      if (file.exists(file_path)) {
        return(readRDS(file_path))
      }
      return(fallback)
    }, error = function(e) {
      message("Error reading RDS file: ", e$message)
      return(fallback)
    })
  }
  
  # Sales by customer - Following data integrity principles for data flow
  sales_by_customer <- reactive({
    # Priority 1: Try app_data database (optimized for app)
    if (!is.null(connections$app_data)) {
      tryCatch({
        result <- dbGetQuery(connections$app_data, "SELECT * FROM customer_dna")
        if (nrow(result) > 0) return(result)
      }, error = function(e) {
        # Continue to next data source
      })
    }
    
    # Priority 2: Try local app_data RDS file
    if (local_data_available()) {
      result <- safe_read_rds("app_data/customer_dna.rds")
      if (!is.null(result) && nrow(result) > 0) return(result)
    }
    
    # Priority 3: Try processed_data database
    if (!is.null(connections$processed_data)) {
      tryCatch({
        result <- dbGetQuery(connections$processed_data, "SELECT * FROM customer_dna")
        if (nrow(result) > 0) return(result)
      }, error = function(e) {
        # Continue to next data source
      })
    }
    
    # Priority 4: Try processed data directory RDS file
    result <- safe_read_rds("data/processed/customer_dna.rds")
    if (!is.null(result) && nrow(result) > 0) return(result)
    
    # Fallback: Sample data if all else fails
    data.frame(
      customer_name = paste0("CUST", 1:100),
      time_first = sample(seq(as.Date('2020/01/01'), as.Date('2022/12/31'), by="day"), 100, replace = TRUE),
      time_first_tonow = sample(30:365, 100, replace = TRUE),
      rvalue = sample(1:90, 100, replace = TRUE),
      rlabel = sample(1:3, 100, replace = TRUE),
      fvalue = sample(1:20, 100, replace = TRUE),
      flabel = sample(1:3, 100, replace = TRUE),
      mvalue = round(runif(100, 50, 500), 2),
      mlabel = sample(1:3, 100, replace = TRUE),
      cai = round(runif(100, 0, 1), 2),
      cailabel = sample(1:3, 100, replace = TRUE),
      ipt_mean = round(runif(100, 30, 180), 1),
      pcv = round(runif(100, 100, 2000), 2),
      clv = round(runif(100, 150, 3000), 2),
      cri = round(runif(100, 0, 1), 2),
      nrec = round(runif(100, 0, 1), 3),
      nesstatus = sample(c("N", "E0", "S1", "S2", "S3"), 100, replace = TRUE, 
                         prob = c(0.3, 0.25, 0.2, 0.15, 0.1)),
      nt = round(runif(100, 20, 200), 2),
      e0t = round(runif(100, 50, 500), 2)
    )
  })
  
  # Sales by time and state - Following data integrity principles for data flow
  sales_by_time_state <- reactive({
    # Priority 1: Try app_data database
    if (!is.null(connections$app_data)) {
      tryCatch({
        result <- dbGetQuery(connections$app_data, "SELECT * FROM sales_by_time_state")
        if (nrow(result) > 0) return(result)
      }, error = function(e) {
        # Continue to next data source
      })
    }
    
    # Priority 2: Try local app_data RDS file
    if (local_data_available()) {
      result <- safe_read_rds("app_data/sales_by_time_state.rds")
      if (!is.null(result) && nrow(result) > 0) return(result)
    }
    
    # Priority 3: Try processed_data database
    if (!is.null(connections$processed_data)) {
      tryCatch({
        result <- dbGetQuery(connections$processed_data, "SELECT * FROM sales_by_time_state")
        if (nrow(result) > 0) return(result)
      }, error = function(e) {
        # Continue to next data source
      })
    }
    
    # Fallback: Sample data
    states <- state_dictionary$abbreviation[1:20]
    times <- seq(as.Date('2022/01/01'), as.Date('2023/12/31'), by="month")
    
    expand.grid(
      state_filter = states,
      time_scale = times
    ) %>%
      mutate(
        total = round(runif(n(), 10000, 100000), 2),
        average = round(runif(n(), 50, 200), 2),
        num_customers = sample(100:1000, n(), replace = TRUE),
        cum_customers = cumsum(num_customers),
        customer_retention_rate = runif(n(), 0.6, 0.9),
        customer_acquisition_rate = runif(n(), 0.1, 0.3)
      )
  })
  
  # NES transition data - Following data integrity principles for data flow
  nes_transition <- reactive({
    # Priority 1: Try app_data database
    if (!is.null(connections$app_data)) {
      tryCatch({
        result <- dbGetQuery(connections$app_data, "SELECT * FROM nes_transition")
        if (nrow(result) > 0) return(result)
      }, error = function(e) {
        # Continue to next data source
      })
    }
    
    # Priority 2: Try local app_data RDS file
    if (local_data_available()) {
      result <- safe_read_rds("app_data/nes_transition.rds")
      if (!is.null(result) && nrow(result) > 0) return(result)
    }
    
    # Priority 3: Try processed_data database
    if (!is.null(connections$processed_data)) {
      tryCatch({
        result <- dbGetQuery(connections$processed_data, "SELECT * FROM nes_transition")
        if (nrow(result) > 0) return(result)
      }, error = function(e) {
        # Continue to next data source
      })
    }
    
    # Fallback: Sample data
    expand.grid(
      nesstatus_pre = c("N", "E0", "S1", "S2", "S3"),
      nesstatus_now = c("N", "E0", "S1", "S2", "S3")
    ) %>%
      mutate(
        count = sample(10:100, n(), replace = TRUE),
        activation_rate = ifelse(nesstatus_pre == "N" & nesstatus_now == "E0", 
                                runif(1, 0.2, 0.4), runif(n(), 0, 1)),
        retention_rate = ifelse(nesstatus_pre == nesstatus_now, 
                               runif(n(), 0.4, 0.8), runif(n(), 0, 0.3))
      )
  })
  
  # Return the list of reactive data sources
  list(
    # Database connections
    connections = connections,
    # Data access functions
    sales_by_customer = sales_by_customer,
    sales_by_time_state = sales_by_time_state,
    nes_transition = nes_transition
  )
}

# Define server logic
server <- function(input, output, session) {
  # Initialize data source
  data_sources <- data_source_function()
  
  # Initialize modules
  sidebarServer("sidebar", data_sources)
  macroOverviewServer("overview", data_sources)
  microCustomerServer("customer", data_sources)
  targetProfilingServer("segmentation", data_sources)
  
  # Handle tab switching for scroll-free UI
  observeEvent(input$main_tabs, {
    # Reset scroll position when switching tabs
    shinyjs::runjs("window.scrollTo(0, 0);")
  })
}

# UI Components for scroll-free experience
ui <- page_navbar(
  title = "AI行銷科技平台", # Generic title for Brand Protection
  theme = bs_theme(
    version = 5,
    bootswatch = "flatly",
    primary = "#2C3E50",
    secondary = "#18BC9C"
  ),
  useShinyjs(), # Enable JavaScript for enhanced UI interactions
  
  # Custom CSS for scroll-free UI
  tags$head(
    tags$style(HTML("
      /* Make card headers more compact */
      .card-header {
        padding: 0.5rem 1rem;
      }
      
      /* Ensure plots don't overflow */
      .plot-container {
        max-height: 400px;
        overflow: hidden;
      }
      
      /* Grid spacing optimizations */
      .grid-card {
        margin-bottom: 0;
      }
      
      /* Fixed height containers */
      .fixed-height-container {
        height: 100%;
        overflow: auto;
      }
      
      /* Sticky navbar */
      .navbar.fixed-top {
        z-index: 1030;
      }
      
      /* Optimize value boxes */
      .value-box {
        min-height: unset;
        margin-bottom: 0.5rem;
      }
      
      /* Make sidebar collapsible */
      @media (max-width: 768px) {
        .sidebar {
          max-height: 50vh;
          overflow-y: auto;
        }
      }
    "))
  ),
  
  # Sidebar from module
  sidebar = sidebarUI("sidebar"),
  
  # Main content panels from modules
  nav_panel(
    title = "儀表板",
    div(
      class = "container-fluid p-0",
      # Use a tabset for second-level navigation within the dashboard
      # This implements the Scroll-Free UI Rule by separating content into tabs
      tabsetPanel(
        id = "dashboard_tabs",
        type = "pills",
        
        # Overview tab - uses macro_overview module
        tabPanel(
          title = "總覽",
          div(class = "mt-3", macroOverviewUI("overview"))
        ),
        
        # Customer tab - uses micro_customer module
        tabPanel(
          title = "客戶分析",
          div(class = "mt-3", microCustomerUI("customer"))
        ),
        
        # Target profiling tab - uses target_profiling module
        tabPanel(
          title = "目標客群",
          div(class = "mt-3", targetProfilingUI("segmentation"))
        )
      )
    )
  ),
  
  # Additional informational panel
  nav_panel(
    title = "使用說明",
    card(
      card_header("平台使用指南"),
      card_body(
        tabsetPanel(
          # Tab navigation for help content implements Scroll-Free UI Rule
          tabPanel(
            title = "功能概述",
            div(
              class = "mt-3",
              h4("AI行銷科技平台功能概述"),
              p("本平台提供全面的客戶分析功能，幫助您了解客戶行為並優化行銷策略。"),
              
              h5("主要功能區域："),
              tags$ul(
                tags$li(strong("總覽："), "查看關鍵績效指標和總體客戶分布"),
                tags$li(strong("客戶分析："), "深入了解個別客戶的行為和價值"),
                tags$li(strong("目標客群："), "識別和分析高價值客戶群體")
              )
            )
          ),
          tabPanel(
            title = "使用技巧",
            div(
              class = "mt-3",
              h4("有效使用平台的技巧"),
              tags$ol(
                tags$li("使用側邊欄篩選器縮小數據範圍"),
                tags$li("點擊圖表元素可查看更詳細資訊"),
                tags$li("使用頂部導航在主要功能區域間切換"),
                tags$li("利用標籤頁在相關內容間導航，無需滾動")
              )
            )
          ),
          tabPanel(
            title = "數據定義",
            div(
              class = "mt-3",
              h4("關鍵指標定義"),
              tags$dl(
                tags$dt("NES狀態"),
                tags$dd("客戶狀態分類：N (新客戶)、E0 (活躍客戶)、S1-S3 (不同程度的靜止客戶)"),
                
                tags$dt("CLV (客戶終身價值)"),
                tags$dd("客戶預期為企業帶來的總收入"),
                
                tags$dt("CAI (客戶活躍度指數)"),
                tags$dd("衡量客戶活躍程度的指標，範圍為0-1"),
                
                tags$dt("CRI (客戶關係穩定度)"),
                tags$dd("衡量客戶購買行為穩定性的指標")
              )
            )
          )
        )
      )
    )
  )
)

# Run the application
shinyApp(ui = ui, server = server)