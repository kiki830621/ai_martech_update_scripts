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

# Source global parameters and brand-specific settings
source(file.path("local_scripts", "brand_specific_parameters.R"))
source(file.path("../../../../global_scripts", "03_config", "global_parameters.R"))
source(file.path("../../../../global_scripts", "11_rshinyapp_utils", "formattime.R"))

# Define label dictionaries (Variable Elements - client-specific terminology)
# These are Variable Elements that can be adapted based on client-specific requirements
# while maintaining the same structure as KitchenMAMA

# Define NES status readable labels 
nes_status_labels <- c(
  "N" = "新客戶",
  "E0" = "活躍客戶",
  "S1" = "初期靜止客戶",
  "S2" = "中期靜止客戶", 
  "S3" = "長期靜止客戶"
)

# Define loyalty tier readable labels
loyalty_tier_labels <- c(
  "Platinum" = "白金會員",
  "Gold" = "黃金會員",
  "Silver" = "白銀會員",
  "Bronze" = "青銅會員"
)

# Define value segment readable labels
value_segment_labels <- c(
  "High Value" = "高價值客戶",
  "Medium-High Value" = "中高價值客戶",
  "Medium Value" = "中價值客戶",
  "Standard Value" = "一般價值客戶"
)

# Load customer DNA data (Variable Element - client-specific data source)
# This function maintains the same structure as KitchenMAMA but adapts to client's data sources
load_customer_dna <- function() {
  # Try to read from RDS file first
  rds_path <- file.path("data", "processed", "customer_dna.rds")
  
  if (file.exists(rds_path)) {
    return(readRDS(rds_path))
  } else {
    # Try to connect to database as fallback
    tryCatch({
      conn <- dbConnect(duckdb(), dbdir = "Data.duckdb")
      if (dbExistsTable(conn, "processed_data.customer_dna")) {
        data <- dbReadTable(conn, "processed_data.customer_dna")
        dbDisconnect(conn)
        return(data)
      } else {
        # Run the analysis if data doesn't exist
        source(file.path("update_scripts", "basic_customer_dna.R"))
        return(readRDS(rds_path))
      }
    }, error = function(e) {
      # Return example data if all else fails
      message("Error loading data: ", e$message)
      message("Loading sample data instead")
      
      # Create sample data
      sample_data <- data.frame(
        customer_id = paste0("customer", 1:50),
        recency = sample(1:180, 50, replace = TRUE),
        frequency = sample(1:10, 50, replace = TRUE),
        monetary = runif(50, 20, 200),
        total_spent = runif(50, 50, 2000),
        first_purchase = as.Date(Sys.Date() - sample(30:365, 50, replace = TRUE)),
        last_purchase = as.Date(Sys.Date() - sample(1:30, 50, replace = TRUE)),
        customer_tenure = sample(30:365, 50, replace = TRUE),
        CLV = runif(50, 100, 3000),
        NESstatus = sample(c("N", "E0", "S1", "S2", "S3"), 50, replace = TRUE),
        value_segment = sample(c("High Value", "Medium-High Value", "Medium Value", "Standard Value"), 50, replace = TRUE),
        loyalty_tier = sample(c("Platinum", "Gold", "Silver", "Bronze"), 50, replace = TRUE)
      )
      
      return(sample_data)
    })
  }
}

# UI Components --------------------------------------------------------------
# IMPORTANT: These UI components follow the Constant Elements principle
# The structure, layout, and organization must be preserved exactly as in KitchenMAMA
# Only the specific values (options, labels) are Variable Elements

# Create the sidebar UI (Constant Element - sidebar structure)
sidebarUI <- function(id) {
  ns <- NS(id)
  
  # Sidebar structure is a Constant Element
  # The specific options (source_dictionary, product_line_dictionary) are Variable Elements
  sidebar(
    title = "選項", # Constant Element - same title as KitchenMAMA
    # Filter components below are Constant Elements in structure
    # Their specific options are Variable Elements adapted for the client
    radioButtons(
      inputId = ns("distribution_channel"),
      label = "行銷通路", # Constant Element - same label as KitchenMAMA
      choices = source_dictionary, # Variable Element - client-specific values
      selected = source_vec[1],
      width = "100%"
    ),
    radioButtons(
      inputId = ns("product_category"),
      label = "商品種類", # Constant Element - same label as KitchenMAMA
      choices = product_line_dictionary, # Variable Element - client-specific values
      selected = product_line_id_vec[1],
      width = "100%"
    ),
    selectInput(
      inputId = ns("time_scale_profile"),
      label = "時間尺度", # Constant Element - same label as KitchenMAMA
      choices = list(
        "年" = "year", 
        "季度" = "quarter", 
        "月" = "month"
      ),
      selected = "quarter"
    ),
    selectizeInput(
      inputId = ns("geo"), 
      label = "地區（州或全部）", # Constant Element - same label as KitchenMAMA
      choices = setNames(
        as.list(state_dictionary$abbreviation), 
        state_dictionary$name
      ), # Variable Element - client-specific values
      selected = "ALL",
      multiple = FALSE,
      options = list(plugins = list('remove_button', 'drag_drop'))
    )
  )
}

# Macro overview UI - shows high-level metrics (Constant Element - panel structure)
# This panel structure must be preserved exactly as in KitchenMAMA
macroOverviewUI <- function(id) {
  ns <- NS(id)
  
  # The entire panel structure is a Constant Element
  # Using nav_panel implements the Scroll-Free UI Rule by containing content in a dedicated tab
  # that doesn't require scrolling to navigate between major sections
  nav_panel(
    title = "總覽", # Constant Element - same title as KitchenMAMA
    layout_columns(
      col_widths = c(12), # Constant Element - same layout as KitchenMAMA
      card(
        full_screen = TRUE,
        card_header("客戶分析總覽"), # Constant Element - same header as KitchenMAMA
        # Value box layout is a Constant Element
        layout_column_wrap(
          width = "250px",
          value_box(
            title = "總客戶數量", # Constant Element - same label as KitchenMAMA
            value = textOutput(ns("total_customers")),
            showcase = bs_icon("people-fill") # Constant Element - same icon as KitchenMAMA
          ),
          value_box(
            title = "活躍客戶", # Constant Element - same label as KitchenMAMA
            value = textOutput(ns("active_customers")),
            showcase = bs_icon("person-check-fill"), # Constant Element - same icon as KitchenMAMA
            p(textOutput(ns("active_percentage"), inline = TRUE), "%")
          ),
          value_box(
            title = "靜止客戶", # Constant Element - same label as KitchenMAMA
            value = textOutput(ns("sleeping_customers")),
            showcase = bs_icon("person-slash"), # Constant Element - same icon as KitchenMAMA
            p(textOutput(ns("sleeping_percentage"), inline = TRUE), "%")
          ),
          value_box(
            title = "新客戶", # Constant Element - same label as KitchenMAMA
            value = textOutput(ns("new_customers")),
            showcase = bs_icon("person-plus-fill"), # Constant Element - same icon as KitchenMAMA
            p(textOutput(ns("new_percentage"), inline = TRUE), "%")
          ),
          value_box(
            title = "平均客戶價值", # Constant Element - same label as KitchenMAMA
            value = textOutput(ns("avg_clv")),
            showcase = bs_icon("cash") # Constant Element - same icon as KitchenMAMA
          ),
          value_box(
            title = "最高客戶價值", # Constant Element - same label as KitchenMAMA
            value = textOutput(ns("max_clv")),
            showcase = bs_icon("gem") # Constant Element - same icon as KitchenMAMA
          )
        ),
        # Chart layout is a Constant Element
        card(
          card_header("客戶狀態分布"), # Constant Element - same header as KitchenMAMA
          layout_columns(
            col_widths = c(6, 6), # Constant Element - same layout as KitchenMAMA
            card(plotlyOutput(ns("nes_status_plot"), height = "300px")), # Constant Element - same chart as KitchenMAMA
            card(plotlyOutput(ns("value_segment_plot"), height = "300px")) # Constant Element - same chart as KitchenMAMA
          ),
          layout_columns(
            col_widths = c(6, 6), # Constant Element - same layout as KitchenMAMA
            card(plotlyOutput(ns("loyalty_tier_plot"), height = "300px")), # Constant Element - same chart as KitchenMAMA
            card(plotlyOutput(ns("loyalty_value_plot"), height = "300px")) # Constant Element - same chart as KitchenMAMA
          )
        )
      )
    )
  )
}

# Micro customer UI - individual customer details (Constant Element - panel structure)
# This panel structure must be preserved exactly as in KitchenMAMA
microCustomerUI <- function(id) {
  ns <- NS(id)
  
  # The entire panel structure is a Constant Element
  # Using nav_panel implements the Scroll-Free UI Rule by organizing customer details
  # in a dedicated tab without requiring scrolling between major functional areas
  nav_panel(
    title = "微觀", # Constant Element - same title as KitchenMAMA
    card(
      full_screen = TRUE,
      card_body(
        grid_container(
          # Grid layout is a Constant Element - exact same layout as KitchenMAMA
          layout = c(
            "area0 area1"
          ),
          row_sizes = c(
            "1fr"
          ),
          col_sizes = c(
            "250px",
            "1.32fr"
          ),
          gap_size = "10px",
          grid_card(
            area = "area0",
            card_body(
              # Filter components below are Constant Elements in structure
              # Their specific options are Variable Elements
              selectizeInput(
                inputId = ns("dna_customer_id"),
                label = "客戶ID", # Constant Element - same label as KitchenMAMA
                choices = NULL,
                multiple = FALSE,
                options = list(plugins = list('remove_button', 'drag_drop'))
              ),
              hr(),
              selectInput(
                inputId = ns("status_filter"),
                label = "狀態篩選", # Constant Element - same label as KitchenMAMA
                # Variable Element - client-specific values but maintaining same structure
                choices = c("全部", "新客戶 (N)", "活躍客戶 (E0)", "靜止客戶 (S1-S3)"),
                selected = "全部"
              ),
              selectInput(
                inputId = ns("value_filter"),
                label = "價值篩選", # Constant Element - same label as KitchenMAMA
                # Variable Element - client-specific values but maintaining same structure
                choices = c("全部", "高價值客戶", "中高價值客戶", "中價值客戶", "一般價值客戶"),
                selected = "全部"
              ),
              selectInput(
                inputId = ns("loyalty_filter"),
                label = "忠誠度篩選", # Constant Element - same label as KitchenMAMA
                # Variable Element - client-specific values but maintaining same structure
                choices = c("全部", "白金會員", "黃金會員", "白銀會員", "青銅會員"),
                selected = "全部"
              )
            )
          ),
          grid_card(
            area = "area1",
            card_body(
              # Value box layout is a Constant Element - same layout as KitchenMAMA
              layout_column_wrap(
                width = "200px",
                fill = FALSE,
                
                # Customer info value boxes - all these are Constant Elements in structure
                # The actual data displayed will be Variable Elements specific to the client
                value_box(
                  title = "顧客資歷", # Constant Element - same label as KitchenMAMA
                  value = textOutput(ns("dna_time_first")),
                  showcase = bs_icon("calendar-event-fill"), # Constant Element - same icon as KitchenMAMA
                  p(textOutput(ns("dna_time_first_tonow"), inline = TRUE), "天")
                ),
                value_box(
                  title = "最近購買日(R)", # Constant Element - same label as KitchenMAMA
                  value = textOutput(ns("dna_recency")),
                  showcase = bs_icon("calendar-event-fill"), # Constant Element - same icon as KitchenMAMA
                  p(textOutput(ns("dna_last_purchase"), inline = TRUE))
                ),
                value_box(
                  title = "購買頻率(F)", # Constant Element - same label as KitchenMAMA
                  value = textOutput(ns("dna_frequency")),
                  showcase = bs_icon("plus-lg"), # Constant Element - same icon as KitchenMAMA
                  p(textOutput(ns("dna_frequency_label"), inline = TRUE))
                ),
                value_box(
                  title = "購買金額(M)", # Constant Element - same label as KitchenMAMA
                  value = textOutput(ns("dna_monetary")),
                  showcase = bs_icon("pie-chart"), # Constant Element - same icon as KitchenMAMA
                  p(textOutput(ns("dna_monetary_label"), inline = TRUE))
                ),
                value_box(
                  title = "顧客終身價值(CLV)", # Constant Element - same label as KitchenMAMA
                  value = div(
                    textOutput(ns("dna_clv"), inline = TRUE),
                    " 美金" # Variable Element - client uses USD
                  ),
                  showcase = bs_icon("pie-chart") # Constant Element - same icon as KitchenMAMA
                ),
                value_box(
                  title = "總消費金額", # Constant Element - same label as KitchenMAMA
                  value = div(
                    textOutput(ns("dna_total_spent"), inline = TRUE),
                    " 美金" # Variable Element - client uses USD
                  ),
                  showcase = bs_icon("pie-chart") # Constant Element - same icon as KitchenMAMA
                ),
                value_box(
                  title = "價值區隔", # Constant Element - same label as KitchenMAMA
                  value = textOutput(ns("dna_value_segment")),
                  showcase = bs_icon("pie-chart") # Constant Element - same icon as KitchenMAMA
                ),
                value_box(
                  title = "忠誠度", # Constant Element - same label as KitchenMAMA
                  value = textOutput(ns("dna_loyalty_tier")),
                  showcase = bs_icon("pie-chart") # Constant Element - same icon as KitchenMAMA
                ),
                value_box(
                  title = "顧客狀態(NES)", # Constant Element - same label as KitchenMAMA
                  value = textOutput(ns("dna_nesstatus")),
                  showcase = bs_icon("pie-chart") # Constant Element - same icon as KitchenMAMA
                )
              )
            )
          )
        )
      )
    )
  )
}

# Marketing target profiling UI (Constant Element - panel structure)
# This panel structure must be preserved exactly as in KitchenMAMA
targetProfilingUI <- function(id) {
  ns <- NS(id)
  
  # The entire panel structure is a Constant Element
  # Using nav_panel implements the Scroll-Free UI Rule by separating the segmentation analysis
  # into its own tab, making all visualizations accessible without scrolling between sections
  nav_panel(
    title = "目標群組", # Constant Element - same title as KitchenMAMA
    card(
      full_screen = TRUE,
      card_header("客戶分群分析"), # Constant Element - same header as KitchenMAMA
      card_body(
        # Chart layout is a Constant Element - must match KitchenMAMA exactly
        layout_columns(
          col_widths = c(6, 6), # Constant Element - same layout as KitchenMAMA
          card(
            card_header("頻率與近期性分析"), # Constant Element - same header as KitchenMAMA
            plotlyOutput(ns("frequency_recency_plot"), height = "400px") # Constant Element - same chart type as KitchenMAMA
          ),
          card(
            card_header("金額與頻率分析"), # Constant Element - same header as KitchenMAMA
            plotlyOutput(ns("monetary_frequency_plot"), height = "400px") # Constant Element - same chart type as KitchenMAMA
          )
        ),
        card(
          card_header("客戶分群分布"), # Constant Element - same header as KitchenMAMA
          plotlyOutput(ns("segment_heatmap"), height = "400px") # Constant Element - same chart type as KitchenMAMA
        ),
        layout_columns(
          col_widths = c(4, 4, 4), # Constant Element - same layout as KitchenMAMA
          card(
            card_header("客戶區隔平均終身價值"), # Constant Element - same header as KitchenMAMA
            plotlyOutput(ns("avg_clv_by_segment"), height = "300px") # Constant Element - same chart type as KitchenMAMA
          ),
          card(
            card_header("客戶區隔平均購買頻率"), # Constant Element - same header as KitchenMAMA
            plotlyOutput(ns("avg_freq_by_segment"), height = "300px") # Constant Element - same chart type as KitchenMAMA
          ),
          card(
            card_header("客戶區隔平均購買間隔"), # Constant Element - same header as KitchenMAMA
            plotlyOutput(ns("avg_recency_by_segment"), height = "300px") # Constant Element - same chart type as KitchenMAMA
          )
        )
      )
    )
  )
}

# Main UI (Constant Element - main app structure)
# The main UI structure must be preserved exactly as in KitchenMAMA
# Using page_sidebar with nav_panels implements the Scroll-Free UI Rule by creating
# a tabbed interface that eliminates the need for scrolling between major functional areas
ui <- page_sidebar(
  title = "AI行銷科技平台", # Implementing Brand Protection Rule - generic name instead of client name
  sidebar = sidebarUI("sidebar"), # Constant Element - same sidebar structure as KitchenMAMA
  macroOverviewUI("overview"), # Constant Element - same panel as KitchenMAMA
  microCustomerUI("customer"), # Constant Element - same panel as KitchenMAMA
  targetProfilingUI("segmentation") # Constant Element - same panel as KitchenMAMA
)

# Server logic ---------------------------------------------------------------
# IMPORTANT: The server logic structure follows the Constant Elements principle
# The reactive data flow pattern must be preserved exactly as in KitchenMAMA
# Only the specific data processing can be adapted for the client

server <- function(input, output, session) {
  # Load data (Variable Element - client-specific data source)
  customer_data <- reactiveVal(load_customer_dna())
  
  # Store filter values (Constant Element - same filter mechanism as KitchenMAMA)
  observe({
    session$userData$filters <- list(
      distribution_channel = input$sidebar_distribution_channel,
      product_category = input$sidebar_product_category,
      time_scale_profile = input$sidebar_time_scale_profile,
      geo = input$sidebar_geo
    )
  })
  
  # Refresh data event (Constant Element - same behavior as KitchenMAMA)
  observeEvent(input$refresh_data, {
    customer_data(load_customer_dna())
  })
  
  # Overview Module Server (Constant Element - server structure)
  # The server structure and reactive data flow must be preserved exactly as in KitchenMAMA
  overviewServer <- function(id) {
    moduleServer(id, function(input, output, session) {
      # Calculate key metrics (Constant Element - same metrics calculation as KitchenMAMA)
      # The specific calculation method is a Constant Element
      metrics <- reactive({
        data <- customer_data()
        
        # Calculate metrics
        active_count <- sum(data$NESstatus %in% c("N", "E0"))
        sleeping_count <- sum(data$NESstatus %in% c("S1", "S2", "S3"))
        new_count <- sum(data$NESstatus == "N")
        total_count <- nrow(data)
        
        list(
          total_customers = total_count,
          active_customers = active_count,
          active_percentage = round(active_count / total_count * 100, 1),
          sleeping_customers = sleeping_count,
          sleeping_percentage = round(sleeping_count / total_count * 100, 1),
          new_customers = new_count,
          new_percentage = round(new_count / total_count * 100, 1),
          avg_clv = mean(data$CLV, na.rm = TRUE),
          max_clv = max(data$CLV, na.rm = TRUE)
        )
      })
      
      # Render value boxes
      output$total_customers <- renderText({
        format(metrics()$total_customers, big.mark = ",")
      })
      
      output$active_customers <- renderText({
        format(metrics()$active_customers, big.mark = ",")
      })
      
      output$active_percentage <- renderText({
        metrics()$active_percentage
      })
      
      output$sleeping_customers <- renderText({
        format(metrics()$sleeping_customers, big.mark = ",")
      })
      
      output$sleeping_percentage <- renderText({
        metrics()$sleeping_percentage
      })
      
      output$new_customers <- renderText({
        format(metrics()$new_customers, big.mark = ",")
      })
      
      output$new_percentage <- renderText({
        metrics()$new_percentage
      })
      
      output$avg_clv <- renderText({
        paste0("$", format(round(metrics()$avg_clv, 2), big.mark = ","))
      })
      
      output$max_clv <- renderText({
        paste0("$", format(round(metrics()$max_clv, 2), big.mark = ","))
      })
      
      # NES Status plot
      output$nes_status_plot <- renderPlotly({
        data <- customer_data() %>%
          count(NESstatus) %>%
          mutate(
            percentage = n / sum(n) * 100,
            NESstatus = factor(NESstatus, levels = c("N", "E0", "S1", "S2", "S3")),
            status_label = nes_status_labels[NESstatus]
          )
        
        p <- ggplot(data, aes(x = NESstatus, y = percentage, fill = NESstatus, text = paste0(status_label, ": ", round(percentage, 1), "%"))) +
          geom_col() +
          geom_text(aes(label = paste0(round(percentage), "%")), vjust = -0.5) +
          labs(title = "客戶狀態分布", x = "狀態", y = "百分比") +
          theme_minimal() +
          scale_fill_brewer(palette = "Blues") +
          scale_x_discrete(labels = nes_status_labels)
        
        ggplotly(p, tooltip = "text") %>% 
          layout(showlegend = FALSE)
      })
      
      # Value Segment plot
      output$value_segment_plot <- renderPlotly({
        data <- customer_data() %>%
          count(value_segment) %>%
          mutate(
            percentage = n / sum(n) * 100,
            value_segment = factor(value_segment, 
                                  levels = c("High Value", "Medium-High Value", 
                                            "Medium Value", "Standard Value")),
            value_label = value_segment_labels[value_segment]
          )
        
        p <- ggplot(data, aes(x = value_segment, y = percentage, fill = value_segment, 
                             text = paste0(value_label, ": ", round(percentage, 1), "%"))) +
          geom_col() +
          geom_text(aes(label = paste0(round(percentage), "%")), vjust = -0.5) +
          labs(title = "價值區隔分布", x = "區隔", y = "百分比") +
          theme_minimal() +
          theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
          scale_fill_brewer(palette = "Greens") +
          scale_x_discrete(labels = value_segment_labels)
        
        ggplotly(p, tooltip = "text") %>% 
          layout(showlegend = FALSE)
      })
      
      # Loyalty Tier plot
      output$loyalty_tier_plot <- renderPlotly({
        data <- customer_data() %>%
          count(loyalty_tier) %>%
          mutate(
            percentage = n / sum(n) * 100,
            loyalty_tier = factor(loyalty_tier, 
                                 levels = c("Platinum", "Gold", "Silver", "Bronze")),
            loyalty_label = loyalty_tier_labels[loyalty_tier]
          )
        
        p <- ggplot(data, aes(x = loyalty_tier, y = percentage, fill = loyalty_tier, 
                             text = paste0(loyalty_label, ": ", round(percentage, 1), "%"))) +
          geom_col() +
          geom_text(aes(label = paste0(round(percentage), "%")), vjust = -0.5) +
          labs(title = "忠誠度分布", x = "會員等級", y = "百分比") +
          theme_minimal() +
          scale_fill_brewer(palette = "Purples") +
          scale_x_discrete(labels = loyalty_tier_labels)
        
        ggplotly(p, tooltip = "text") %>% 
          layout(showlegend = FALSE)
      })
      
      # Loyalty vs Value plot
      output$loyalty_value_plot <- renderPlotly({
        data <- customer_data() %>%
          count(loyalty_tier, value_segment) %>%
          mutate(
            loyalty_tier = factor(loyalty_tier, 
                                 levels = c("Platinum", "Gold", "Silver", "Bronze")),
            value_segment = factor(value_segment, 
                                  levels = c("High Value", "Medium-High Value", 
                                            "Medium Value", "Standard Value")),
            loyalty_label = loyalty_tier_labels[loyalty_tier],
            value_label = value_segment_labels[value_segment]
          )
        
        p <- ggplot(data, aes(x = loyalty_tier, y = n, fill = value_segment, 
                             text = paste0(loyalty_label, ", ", value_label, ": ", n, " 客戶"))) +
          geom_col(position = "dodge") +
          labs(title = "忠誠度與價值分布", 
               x = "忠誠度", 
               y = "客戶數量",
               fill = "價值區隔") +
          theme_minimal() +
          scale_fill_brewer(palette = "Greens", labels = value_segment_labels) +
          scale_x_discrete(labels = loyalty_tier_labels)
        
        ggplotly(p, tooltip = "text")
      })
    })
  }
  
  # Customer Module Server
  customerServer <- function(id) {
    moduleServer(id, function(input, output, session) {
      # Filter customer data based on selections
      filtered_customers <- reactive({
        data <- customer_data()
        
        # Apply status filter
        if (input$status_filter != "全部") {
          status_map <- list(
            "新客戶 (N)" = "N",
            "活躍客戶 (E0)" = "E0",
            "靜止客戶 (S1-S3)" = c("S1", "S2", "S3")
          )
          data <- data %>% filter(NESstatus %in% status_map[[input$status_filter]])
        }
        
        # Map UI filter values to data values for value segments
        value_map <- c(
          "高價值客戶" = "High Value",
          "中高價值客戶" = "Medium-High Value",
          "中價值客戶" = "Medium Value",
          "一般價值客戶" = "Standard Value"
        )
        
        # Apply value filter
        if (input$value_filter != "全部") {
          data <- data %>% filter(value_segment == value_map[input$value_filter])
        }
        
        # Map UI filter values to data values for loyalty tiers
        loyalty_map <- c(
          "白金會員" = "Platinum",
          "黃金會員" = "Gold",
          "白銀會員" = "Silver",
          "青銅會員" = "Bronze"
        )
        
        # Apply loyalty filter
        if (input$loyalty_filter != "全部") {
          data <- data %>% filter(loyalty_tier == loyalty_map[input$loyalty_filter])
        }
        
        return(data)
      })
      
      # Update customer selection dropdown
      observe({
        customers <- filtered_customers()
        
        updateSelectizeInput(
          session,
          "dna_customer_id",
          choices = customers$customer_id,
          selected = if(length(customers$customer_id) > 0) customers$customer_id[1] else NULL
        )
      })
      
      # Selected customer data
      selected_customer <- reactive({
        req(input$dna_customer_id)
        filtered_customers() %>%
          filter(customer_id == input$dna_customer_id)
      })
      
      # Render customer detail outputs
      output$dna_nesstatus <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        nes_status_labels[customer$NESstatus]
      })
      
      output$dna_value_segment <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        value_segment_labels[customer$value_segment]
      })
      
      output$dna_loyalty_tier <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        loyalty_tier_labels[customer$loyalty_tier]
      })
      
      output$dna_time_first <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        if ("first_purchase" %in% names(customer)) {
          format(as.Date(customer$first_purchase), "%Y-%m-%d")
        } else {
          "未知"
        }
      })
      
      output$dna_time_first_tonow <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("0")
        if ("customer_tenure" %in% names(customer)) {
          as.character(round(customer$customer_tenure))
        } else {
          as.character(round(as.numeric(difftime(Sys.Date(), as.Date(customer$first_purchase), units = "days"))))
        }
      })
      
      output$dna_recency <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        textRlabel[round(ntile(customer$recency, 3))]
      })
      
      output$dna_last_purchase <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("")
        if ("last_purchase" %in% names(customer)) {
          format(as.Date(customer$last_purchase), "%Y-%m-%d")
        } else {
          paste0(customer$recency, " 天前")
        }
      })
      
      output$dna_frequency <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        paste0(customer$frequency, " 次")
      })
      
      output$dna_frequency_label <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("")
        textFlabel[min(3, ceiling(customer$frequency/2))]
      })
      
      output$dna_monetary <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        paste0("$", format(round(customer$monetary, 2), big.mark = ","))
      })
      
      output$dna_monetary_label <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("")
        textMlabel[round(ntile(customer$monetary, 3))]
      })
      
      output$dna_total_spent <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        format(round(customer$total_spent, 2), big.mark = ",")
      })
      
      output$dna_clv <- renderText({
        customer <- selected_customer()
        if (nrow(customer) == 0) return("N/A")
        format(round(customer$CLV, 2), big.mark = ",")
      })
    })
  }
  
  # Segmentation Module Server
  segmentationServer <- function(id) {
    moduleServer(id, function(input, output, session) {
      # Frequency vs Recency plot
      output$frequency_recency_plot <- renderPlotly({
        data <- customer_data()
        
        p <- ggplot(data, aes(x = recency, y = frequency, color = NESstatus, 
                             text = paste0("客戶ID: ", customer_id,
                                          "<br>狀態: ", nes_status_labels[NESstatus],
                                          "<br>購買間隔: ", recency, " 天",
                                          "<br>購買頻率: ", frequency, " 次"))) +
          geom_point(alpha = 0.7) +
          labs(title = "購買頻率與近期性分析",
               x = "最近購買間隔 (天)",
               y = "購買次數",
               color = "客戶狀態") +
          theme_minimal() +
          scale_color_brewer(palette = "Set1", labels = nes_status_labels)
        
        ggplotly(p, tooltip = "text")
      })
      
      # Monetary vs Frequency plot
      output$monetary_frequency_plot <- renderPlotly({
        data <- customer_data()
        
        p <- ggplot(data, aes(x = frequency, y = monetary, color = value_segment, 
                             text = paste0("客戶ID: ", customer_id,
                                          "<br>價值區隔: ", value_segment_labels[value_segment],
                                          "<br>購買頻率: ", frequency, " 次",
                                          "<br>平均消費: $", round(monetary, 2)))) +
          geom_point(alpha = 0.7) +
          labs(title = "平均消費與購買頻率分析",
               x = "購買次數",
               y = "平均消費金額 ($)",
               color = "價值區隔") +
          theme_minimal() +
          scale_color_brewer(palette = "Set2", labels = value_segment_labels)
        
        ggplotly(p, tooltip = "text")
      })
      
      # Segment heatmap
      output$segment_heatmap <- renderPlotly({
        data <- customer_data() %>%
          count(NESstatus, loyalty_tier) %>%
          mutate(
            NESstatus = factor(NESstatus, levels = c("N", "E0", "S1", "S2", "S3")),
            loyalty_tier = factor(loyalty_tier, levels = c("Platinum", "Gold", "Silver", "Bronze")),
            status_label = nes_status_labels[NESstatus],
            loyalty_label = loyalty_tier_labels[loyalty_tier]
          )
        
        p <- ggplot(data, aes(x = NESstatus, y = loyalty_tier, fill = n, 
                             text = paste0(status_label, " + ", loyalty_label, ": ", n, " 客戶"))) +
          geom_tile() +
          geom_text(aes(label = n), color = "white") +
          scale_fill_viridis_c() +
          labs(title = "客戶分群分布熱圖",
               x = "客戶狀態",
               y = "忠誠度",
               fill = "客戶數量") +
          theme_minimal() +
          scale_x_discrete(labels = nes_status_labels) +
          scale_y_discrete(labels = loyalty_tier_labels)
        
        ggplotly(p, tooltip = "text")
      })
      
      # Average CLV by segment
      output$avg_clv_by_segment <- renderPlotly({
        data <- customer_data() %>%
          group_by(NESstatus) %>%
          summarize(avg_clv = mean(CLV, na.rm = TRUE)) %>%
          mutate(
            NESstatus = factor(NESstatus, levels = c("N", "E0", "S1", "S2", "S3")),
            status_label = nes_status_labels[NESstatus]
          )
        
        p <- ggplot(data, aes(x = NESstatus, y = avg_clv, fill = NESstatus, 
                             text = paste0(status_label, ": $", round(avg_clv, 2)))) +
          geom_col() +
          geom_text(aes(label = paste0("$", round(avg_clv))), vjust = -0.5) +
          labs(title = "各狀態平均終身價值",
               x = "客戶狀態",
               y = "平均CLV ($)") +
          theme_minimal() +
          theme(legend.position = "none") +
          scale_fill_brewer(palette = "Blues") +
          scale_x_discrete(labels = nes_status_labels)
        
        ggplotly(p, tooltip = "text")
      })
      
      # Average Frequency by segment
      output$avg_freq_by_segment <- renderPlotly({
        data <- customer_data() %>%
          group_by(value_segment) %>%
          summarize(avg_freq = mean(frequency, na.rm = TRUE)) %>%
          mutate(
            value_segment = factor(value_segment, 
                                 levels = c("High Value", "Medium-High Value", 
                                           "Medium Value", "Standard Value")),
            value_label = value_segment_labels[value_segment]
          )
        
        p <- ggplot(data, aes(x = value_segment, y = avg_freq, fill = value_segment, 
                             text = paste0(value_label, ": ", round(avg_freq, 1), " 次"))) +
          geom_col() +
          geom_text(aes(label = round(avg_freq, 1)), vjust = -0.5) +
          labs(title = "各價值區隔平均購買頻率",
               x = "價值區隔",
               y = "平均購買次數") +
          theme_minimal() +
          theme(legend.position = "none", 
                axis.text.x = element_text(angle = 45, hjust = 1)) +
          scale_fill_brewer(palette = "Greens") +
          scale_x_discrete(labels = value_segment_labels)
        
        ggplotly(p, tooltip = "text")
      })
      
      # Average Recency by segment
      output$avg_recency_by_segment <- renderPlotly({
        data <- customer_data() %>%
          group_by(loyalty_tier) %>%
          summarize(avg_recency = mean(recency, na.rm = TRUE)) %>%
          mutate(
            loyalty_tier = factor(loyalty_tier, 
                               levels = c("Platinum", "Gold", "Silver", "Bronze")),
            loyalty_label = loyalty_tier_labels[loyalty_tier]
          )
        
        p <- ggplot(data, aes(x = loyalty_tier, y = avg_recency, fill = loyalty_tier, 
                             text = paste0(loyalty_label, ": ", round(avg_recency), " 天"))) +
          geom_col() +
          geom_text(aes(label = round(avg_recency)), vjust = -0.5) +
          labs(title = "各忠誠度平均購買間隔",
               x = "忠誠度",
               y = "平均購買間隔 (天)") +
          theme_minimal() +
          theme(legend.position = "none") +
          scale_fill_brewer(palette = "Purples") +
          scale_x_discrete(labels = loyalty_tier_labels)
        
        ggplotly(p, tooltip = "text")
      })
    })
  }
  
  # Initialize module servers
  overviewServer("overview")
  customerServer("customer")
  segmentationServer("segmentation")
}

# Run the app
shinyApp(ui = ui, server = server)