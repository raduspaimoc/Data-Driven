CREATE TABLE [dbo].[dim_companies](
	[company_id] [int] NOT NULL,
	[company_name] [varchar](255) NULL,
	[company_catchPhrase] [varchar](255) NULL,
	[company_bs] [varchar](255) NULL,
PRIMARY KEY CLUSTERED
(
	[company_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


CREATE TABLE [dbo].[dim_dates](
	[date_id] [int] NOT NULL,
	[arrival_date] [date] NULL,
PRIMARY KEY CLUSTERED
(
	[date_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


CREATE TABLE [dbo].[dim_hotels](
	[hotel_id] [int] NOT NULL,
	[hotel] [varchar](255) NULL,
PRIMARY KEY CLUSTERED
(
	[hotel_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[dim_meals](
	[meal_id] [int] NOT NULL,
	[meal] [varchar](255) NULL,
PRIMARY KEY CLUSTERED
(
	[meal_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[dim_users](
	[id] [int] NOT NULL,
	[name] [varchar](255) NULL,
	[username] [varchar](255) NULL,
	[email] [varchar](255) NULL,
	[phone] [varchar](255) NULL,
	[website] [varchar](255) NULL,
	[street] [varchar](255) NULL,
	[suite] [varchar](255) NULL,
	[city] [varchar](255) NULL,
	[zipcode] [varchar](255) NULL,
	[geo_lat] [float] NULL,
	[geo_lng] [float] NULL,
	[company_id] [int] NULL,
PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[dim_users]  WITH CHECK ADD FOREIGN KEY([company_id])
REFERENCES [dbo].[dim_companies] ([company_id])
GO

CREATE TABLE [dbo].[fact_bookings](
	[booking_id] [int] NOT NULL,
	[agent_id] [int] NULL,
	[hotel_id] [int] NULL,
	[meal_id] [int] NULL,
	[is_canceled] [int] NULL,
	[lead_time] [int] NULL,
	[stays_in_weekend_nights] [int] NULL,
	[stays_in_week_nights] [int] NULL,
	[adults] [int] NULL,
	[children] [int] NULL,
	[is_repeated_guest] [int] NULL,
	[previous_cancellations] [int] NULL,
	[previous_bookings_not_canceled] [int] NULL,
	[reserved_room_type] [varchar](255) NULL,
	[assigned_room_type] [varchar](255) NULL,
	[reservation_status] [varchar](255) NULL,
	[reservation_status_date] [date] NULL,
	[arrival_date_id] [int] NULL,
	[country] [varchar](255) NULL,
 CONSTRAINT [PK__fact_boo__5DE3A5B1B8C8967F] PRIMARY KEY CLUSTERED
(
	[booking_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[fact_bookings]  WITH CHECK ADD  CONSTRAINT [FK__fact_book__arriv__37A5467C] FOREIGN KEY([arrival_date_id])
REFERENCES [dbo].[dim_dates] ([date_id])
GO

ALTER TABLE [dbo].[fact_bookings] CHECK CONSTRAINT [FK__fact_book__arriv__37A5467C]
GO

ALTER TABLE [dbo].[fact_bookings]  WITH CHECK ADD  CONSTRAINT [FK__fact_book__hotel__35BCFE0A] FOREIGN KEY([hotel_id])
REFERENCES [dbo].[dim_hotels] ([hotel_id])
GO

ALTER TABLE [dbo].[fact_bookings] CHECK CONSTRAINT [FK__fact_book__hotel__35BCFE0A]
GO

ALTER TABLE [dbo].[fact_bookings]  WITH CHECK ADD  CONSTRAINT [FK__fact_book__meal___36B12243] FOREIGN KEY([meal_id])
REFERENCES [dbo].[dim_meals] ([meal_id])
GO

ALTER TABLE [dbo].[fact_bookings] CHECK CONSTRAINT [FK__fact_book__meal___36B12243]
GO

ALTER TABLE [dbo].[fact_bookings]  WITH CHECK ADD  CONSTRAINT [FK__fact_book__user___34C8D9D1] FOREIGN KEY([agent_id])
REFERENCES [dbo].[dim_users] ([id])
GO

ALTER TABLE [dbo].[fact_bookings] CHECK CONSTRAINT [FK__fact_book__user___34C8D9D1]
GO