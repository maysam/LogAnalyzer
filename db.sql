USE [GEMPRDv2]
GO
/****** Object:  Table [dbo].[rConstructSim]    Script Date: 4/23/2014 12:54:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[rConstructSim](
	[rConstructSimId] [int] IDENTITY(1,1) NOT NULL,
	[FocalKey] [int] NOT NULL,
	[RelationshipType] [char](1) NOT NULL,
	[RecommendationNo] [int] NOT NULL,
	[RelatedId] [int] NOT NULL,
	[Score] [real] NOT NULL,
 CONSTRAINT [PK_rConstructSim] PRIMARY KEY CLUSTERED 
(
	[rConstructSimId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[rImportedLogs]    Script Date: 4/23/2014 12:54:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[rImportedLogs](
	[dateTimeAccessed] [datetime] NULL,
	[IpAddress] [nchar](20) NULL,
	[itemTYPE] [char](1) NULL,
	[itemID] [int] NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[rMeasureSim]    Script Date: 4/23/2014 12:54:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[rMeasureSim](
	[rMeasureSimId] [int] IDENTITY(1,1) NOT NULL,
	[FocalKey] [int] NOT NULL,
	[RelationshipType] [char](1) NOT NULL,
	[RecommendationNo] [int] NOT NULL,
	[RelatedId] [int] NOT NULL,
	[Score] [real] NOT NULL,
 CONSTRAINT [PK_rMeasureSim] PRIMARY KEY CLUSTERED 
(
	[rMeasureSimId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[rReadFiles]    Script Date: 4/23/2014 12:54:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[rReadFiles](
	[filename] [nchar](100) NULL,
	[last_edit] [datetime] NULL
) ON [PRIMARY]

GO
/****** Object:  StoredProcedure [dbo].[rSynonymousConstructRetrieval]    Script Date: 4/23/2014 12:54:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[rSynonymousConstructRetrieval]
	@key int
AS
BEGIN
	SET NOCOUNT ON;
	Select top 10 RelatedId, C.name from (
		SELECT top 5 *, case when RecommendationNo <= 3 then RecommendationNo else 10 end as X, newid() as new_id  
		from rConstructSim 
		where @key = FocalKey and RelationshipType='s' order by x, new_id
		union 
		SELECT top 5 *, case when RecommendationNo <= 3 then RecommendationNo else 10 end as X, newid() as new_id  
		from rConstructSim 
		where @key = FocalKey and RelationshipType='r' order by x, new_id
	) T left join tConstruct C on C.constructID = RelatedId
END

GO
/****** Object:  StoredProcedure [dbo].[rSynonymousMeasureRetrieval]    Script Date: 4/23/2014 12:54:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[rSynonymousMeasureRetrieval]
	@key int
AS
BEGIN
	SET NOCOUNT ON;
	Select top 10 RelatedId, M.name from (
		SELECT top 5 *, case when RecommendationNo <= 3 then RecommendationNo else 10 end as X, newid() as new_id  
		from rMeasureSim
		where @key = FocalKey and RelationshipType='s' order by x, new_id
		union 
		SELECT top 5 *, case when RecommendationNo <= 3 then RecommendationNo else 10 end as X, newid() as new_id  
		from rMeasureSim
		where @key = FocalKey and RelationshipType='r' order by x, new_id
	) T left join tMeasure M on M.MeasureID = RelatedId
END

GO
