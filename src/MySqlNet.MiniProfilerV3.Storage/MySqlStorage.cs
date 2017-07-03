using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using Dapper;
using MySql.Data.MySqlClient;
using StackExchange.Profiling;
using StackExchange.Profiling.Helpers;
using StackExchange.Profiling.Storage;

namespace MySqlNet.MiniProfilerV3.Storage
{
	public class MySqlStorage : IStorage
	{
		public MySqlStorage(string connectionString)
		{
			m_connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
			m_setProfilerMethodInfo = typeof(Timing).GetProperty("Profiler", BindingFlags.Instance | BindingFlags.NonPublic).SetMethod;
		}

		public IEnumerable<Guid> List(int maxResults, DateTime? start = null, DateTime? finish = null, ListResultsOrder orderBy = ListResultsOrder.Descending)
		{
			var sb = new StringBuilder(@"SELECT Id FROM MiniProfilers ");
			if (finish != null)
				sb.AppendLine("WHERE Started < @finish");
			if (start != null)
				sb.AppendLine(finish != null ? " AND Started > @start" : "WHERE Started > @start");
			sb.Append("ORDER BY ").Append(orderBy == ListResultsOrder.Descending ? "Started DESC" : "Started ASC");
			sb.Append(" LIMIT @maxResults");
			var query = sb.ToString();
			
			using (var conn = CreateConnection())
				return conn.Query<Guid>(query, new { maxResults, start, finish }).AsList();
		}

		public void Save(MiniProfiler profiler)
		{
			using (var conn = CreateConnection())
			{
				conn.Execute(@"INSERT IGNORE INTO MiniProfilers
(Id, RootTimingId, Name, Started, DurationMilliseconds, User, HasUserViewed, MachineName, CustomLinksJson, ClientTimingsRedirectCount)
VALUES(@Id, @RootTimingId, @Name, @Started, @DurationMilliseconds, @User, @HasUserViewed, @MachineName, @CustomLinksJson, @ClientTimingsRedirectCount)",
					new
					{
						profiler.Id,
						profiler.Started,
						Name = profiler.Name.Truncate(200),
						User = profiler.User.Truncate(100),
						RootTimingId = profiler.Root?.Id,
						profiler.DurationMilliseconds,
						profiler.HasUserViewed,
						MachineName = profiler.MachineName.Truncate(100),
						profiler.CustomLinksJson,
						ClientTimingsRedirectCount = profiler.ClientTimings?.RedirectCount
					});

				var timings = new List<Timing>();
				if (profiler.Root != null)
				{
					profiler.Root.MiniProfilerId = profiler.Id;
					FlattenTimings(profiler.Root, timings);
				}

				conn.Execute(@"
INSERT IGNORE INTO MiniProfilerTimings
(Id, MiniProfilerId, ParentTimingId, Name, DurationMilliseconds, StartMilliseconds, IsRoot, Depth, CustomTimingsJson)
VALUES(@Id, @MiniProfilerId, @ParentTimingId, @Name, @DurationMilliseconds, @StartMilliseconds, @IsRoot, @Depth, @CustomTimingsJson)",
					timings.Select(timing => new
					{
						timing.Id,
						timing.MiniProfilerId,
						ParentTimingId = timing.ParentTiming?.Id,
						Name = timing.Name.Truncate(200),
						timing.DurationMilliseconds,
						timing.StartMilliseconds,
						IsRoot = timing.ParentTiming == null && timing.IsRoot,
						timing.Depth,
						timing.CustomTimingsJson
					}));

				if (profiler.ClientTimings?.Timings?.Any() ?? false)
				{
					// set the profilerId (isn't needed unless we are storing it)
					foreach (var timing in profiler.ClientTimings.Timings)
					{
						timing.MiniProfilerId = profiler.Id;
						timing.Id = Guid.NewGuid();
					}

					conn.Execute(@"
INSERT IGNORE INTO MiniProfilerClientTimings
(Id, MiniProfilerId, Name, Start, Duration)
VALUES(@Id, @MiniProfilerId, @Name, @Start, @Duration)",
						profiler.ClientTimings.Timings.Select(timing => new
						{
							timing.Id,
							timing.MiniProfilerId,
							Name = timing.Name.Truncate(200),
							timing.Start,
							timing.Duration
						}));
				}
			}
		}

		public MiniProfiler Load(Guid id)
		{
			MiniProfiler profiler;
			using (var conn = CreateConnection())
			{
				using (var multi = conn.QueryMultiple(@"
SELECT * FROM MiniProfilers WHERE Id = @id;
SELECT * FROM MiniProfilerTimings WHERE MiniProfilerId = @id ORDER BY StartMilliseconds;
SELECT * FROM MiniProfilerClientTimings WHERE MiniProfilerId = @id ORDER BY Start;",
					new { id }))
				{
					profiler = multi.ReadSingleOrDefault<MiniProfiler>();
					var timings = multi.Read<Timing>().AsList();
					var clientTimings = multi.Read<ClientTimings.ClientTiming>().AsList();

					if (profiler?.RootTimingId != null && timings.Any())
					{
						var setProfilerParameters = new object[] { profiler };
						var rootTiming = timings.SingleOrDefault(x => x.Id == profiler.RootTimingId.Value);
						if (rootTiming != null)
						{
							profiler.Root = rootTiming;

							// HACK: Profiler.Timing is internal; use reflection to set it
							foreach (var timing in timings)
								m_setProfilerMethodInfo.Invoke(timing, setProfilerParameters);

							timings.Remove(rootTiming);
							var timingsLookupByParent = timings.ToLookup(x => x.ParentTimingId, x => x);
							PopulateChildTimings(rootTiming, timingsLookupByParent);
						}
						if (clientTimings.Any() || profiler.ClientTimingsRedirectCount.HasValue)
						{
							profiler.ClientTimings = new ClientTimings
							{
								RedirectCount = profiler.ClientTimingsRedirectCount ?? 0,
								Timings = clientTimings
							};
						}
					}
				}
			}

			if (profiler != null)
			{
				// HACK: stored dates are utc, but are pulled out as unspecified
				profiler.Started = new DateTime(profiler.Started.Ticks, DateTimeKind.Utc);
			}
			return profiler;
		}

		public void SetUnviewed(string user, Guid id) => ToggleViewed(user, id, false);

		public void SetViewed(string user, Guid id) => ToggleViewed(user, id, true);

		private void ToggleViewed(string user, Guid id, bool hasUserViewed)
		{
			using (var conn = CreateConnection())
				conn.Execute(@"UPDATE MiniProfilers SET HasUserViewed = @hasUserViewed WHERE Id = @id AND User = @user", new { id, user, hasUserViewed });
		}

		public List<Guid> GetUnviewedIds(string user)
		{
			using (var conn = CreateConnection())
				return conn.Query<Guid>(@"SELECT Id FROM MiniProfilers WHERE User = @user AND HasUserViewed = 0 ORDER BY Started", new { user }).AsList();
		}

		private DbConnection CreateConnection() => new MySqlConnection(m_connectionString);

		/// <summary>
		/// Flattems the timings down into a single list.
		/// </summary>
		/// <param name="timing">The <see cref="Timing"/> to flatten into <paramref name="timingsCollection"/>.</param>
		/// <param name="timingsCollection">The collection to add all timings in the <paramref name="timing"/> tree to.</param>
		private static void FlattenTimings(Timing timing, List<Timing> timingsCollection)
		{
			timingsCollection.Add(timing);
			if (timing.HasChildren)
			{
				foreach (var child in timing.Children)
				{
					FlattenTimings(child, timingsCollection);
				}
			}
		}

		/// <summary>
		/// Build the subtree of <see cref="Timing"/> objects with <paramref name="parent"/> at the top.
		/// Used recursively.
		/// </summary>
		/// <param name="parent">Parent <see cref="Timing"/> to be evaluated.</param>
		/// <param name="timingsLookupByParent">Key: parent timing Id; Value: collection of all <see cref="Timing"/> objects under the given parent.</param>
		private void PopulateChildTimings(Timing parent, ILookup<Guid, Timing> timingsLookupByParent)
		{
			if (timingsLookupByParent.Contains(parent.Id))
			{
				foreach (var timing in timingsLookupByParent[parent.Id].OrderBy(x => x.StartMilliseconds))
				{
					parent.AddChild(timing);
					PopulateChildTimings(timing, timingsLookupByParent);
				}
			}
		}

		/// <summary>
		/// Creates needed tables. Run this once on your database.
		/// </summary>
		/// <remarks>
		/// Works in SQL server and <c>sqlite</c> (with documented removals).
		/// </remarks>
		public const string TableCreationScript = @"
			create table MiniProfilers
			(
				RowId integer not null auto_increment primary key,
				Id char(36) not null collate ascii_general_ci,
				RootTimingId char(36) null collate ascii_general_ci,
				Name varchar(200) null,
				Started datetime not null,
				DurationMilliseconds decimal(7, 1) not null,
				User varchar(100) null,
				HasUserViewed bool not null,
				MachineName varchar(100) null,
				CustomLinksJson longtext,
				ClientTimingsRedirectCount int null,
				unique index IX_MiniProfilers_Id (Id), -- displaying results selects everything based on the main MiniProfilers.Id column
				index IX_MiniProfilers_User_HasUserViewed (User, HasUserViewed) -- speeds up a query that is called on every .Stop()
			) engine=InnoDB collate utf8mb4_bin;

			create table MiniProfilerTimings
			(
				RowId integer not null auto_increment primary key,
				Id char(36) not null collate ascii_general_ci,
				MiniProfilerId char(36) not null collate ascii_general_ci,
				ParentTimingId char(36) null collate ascii_general_ci,
				Name varchar(200) not null,
				DurationMilliseconds decimal(9, 3) not null,
				StartMilliseconds decimal(9, 3) not null,
				IsRoot bool not null,
				Depth smallint not null,
				CustomTimingsJson longtext null,
				unique index IX_MiniProfilerTimings_Id (Id),
				index IX_MiniProfilerTimings_MiniProfilerId (MiniProfilerId)
			) engine=InnoDB collate utf8mb4_bin;

			create table MiniProfilerClientTimings
			(
				RowId integer not null auto_increment primary key,
				Id char(36) not null collate ascii_general_ci,
				MiniProfilerId char(36) not null collate ascii_general_ci,
				Name varchar(200) not null,
				Start decimal(9, 3) not null,
				Duration decimal(9, 3) not null,
				unique index IX_MiniProfilerClientTimings_Id (Id),
				index IX_MiniProfilerClientTimings_MiniProfilerId (MiniProfilerId)
			) engine=InnoDB collate utf8mb4_bin;
			";

		readonly string m_connectionString;
		readonly MethodInfo m_setProfilerMethodInfo;
	}
}
