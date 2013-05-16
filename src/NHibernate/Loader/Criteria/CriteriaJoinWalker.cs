using System.Collections.Generic;
using Iesi.Collections.Generic;

using NHibernate.Engine;
using NHibernate.Persister.Collection;
using NHibernate.Persister.Entity;
using NHibernate.SqlCommand;
using NHibernate.Type;
using NHibernate.Util;

using System.Linq;

namespace NHibernate.Loader.Criteria
{
	/// <summary>
	/// A <see cref="JoinWalker" /> for <see cref="ICriteria" /> queries.
	/// </summary>
	/// <seealso cref="CriteriaLoader"/>
	public class CriteriaJoinWalker : AbstractEntityJoinWalker
	{
		//TODO: add a CriteriaImplementor interface
		//      this class depends directly upon CriteriaImpl in the impl package...

		private readonly CriteriaQueryTranslator translator;
		private readonly ISet<string> querySpaces;
		private readonly IType[] resultTypes;
		//the user visible aliases, which are unknown to the superclass,
		//these are not the actual "physical" SQL aliases
		private readonly string[] userAliases;
		private readonly IList<string> userAliasList = new List<string>();

		private static readonly IInternalLogger logger = LoggerProvider.LoggerFor(typeof(CriteriaJoinWalker));

		public CriteriaJoinWalker(IOuterJoinLoadable persister, CriteriaQueryTranslator translator,
		                          ISessionFactoryImplementor factory, ICriteria criteria, string rootEntityName,
		                          IDictionary<string, IFilter> enabledFilters)
			: base(translator.RootSQLAlias, persister, factory, enabledFilters)
		{
			this.translator = translator;

			querySpaces = translator.GetQuerySpaces();

			if (translator.HasProjection)
			{
				resultTypes = translator.ProjectedTypes;

				InitProjection(
					translator.GetSelect(enabledFilters),
					translator.GetWhereCondition(enabledFilters),
					translator.GetOrderBy(),
					translator.GetGroupBy(),
					translator.GetHavingCondition(enabledFilters),
					enabledFilters, 
					LockMode.None);
			}
			else
			{
				resultTypes = new IType[] {TypeFactory.ManyToOne(persister.EntityName)};

				InitAll(translator.GetWhereCondition(enabledFilters), translator.GetOrderBy(), LockMode.None);
			}

			userAliasList.Add(criteria.Alias); //root entity comes *last*
			userAliases = ArrayHelper.ToStringArray(userAliasList);
		}

		protected override void WalkEntityTree(IOuterJoinLoadable persister, string userAlias, string sqlAlias, string path, int currentDepth)
		{
			// NH different behavior (NH-1476, NH-1760, NH-1785)
            base.WalkEntityTree(persister, userAlias, sqlAlias, path, currentDepth);
            WalkCompositeComponentIdTree(persister, userAlias, sqlAlias, path);
		}

        private void WalkCompositeComponentIdTree(IOuterJoinLoadable persister, string userAlias, string sqlAlias, string path)
		{
			IType type = persister.IdentifierType;
			string propertyName = persister.IdentifierPropertyName;
			if (type != null && type.IsComponentType && !(type is EmbeddedComponentType))
			{
                ILhsAssociationTypeSqlInfo associationTypeSQLInfo = JoinHelper.GetIdLhsSqlInfo(sqlAlias, persister, Factory);
                WalkComponentTree((IAbstractComponentType)type, 0, userAlias, sqlAlias, SubPath(path, propertyName), 0, associationTypeSQLInfo);
			}
		}

		public IType[] ResultTypes
		{
			get { return resultTypes; }
		}

		public string[] UserAliases
		{
			get { return userAliases; }
		}

		/// <summary>
		/// Use the discriminator, to narrow the select to instances
		/// of the queried subclass, also applying any filters.
		/// </summary>
		protected override SqlString WhereFragment
		{
			get { return base.WhereFragment.Append(((Persister.Entity.IQueryable) Persister).FilterFragment(Alias, EnabledFilters)); }
		}

		public ISet<string> QuerySpaces
		{
			get { return querySpaces; }
		}

		public override string Comment
		{
			get { return "criteria query"; }
		}

		protected override JoinType GetJoinType(IAssociationType type, FetchMode config, string path, string lhsTable,
		                                        string[] lhsColumns, bool nullable, int currentDepth,
		                                        CascadeStyle cascadeStyle)
		{
			if (translator.IsJoin(path))
			{
				return translator.GetJoinType(path);
			}
			else
			{
				if (translator.HasProjection)
				{
					return JoinType.None;
				}
				else
				{
					FetchMode fetchMode = translator.RootCriteria.GetFetchMode(path);
					if (IsDefaultFetchMode(fetchMode))
					{
						return base.GetJoinType(type, config, path, lhsTable, lhsColumns, nullable, currentDepth, cascadeStyle);
					}
					else
					{
						if (fetchMode == FetchMode.Join)
						{
							IsDuplicateAssociation(lhsTable, lhsColumns, type); //deliberately ignore return value!
							return GetJoinType(nullable, currentDepth);
						}
						else
						{
							return JoinType.None;
						}
					}
				}
			}
		}

		private static bool IsDefaultFetchMode(FetchMode fetchMode)
		{
			return fetchMode == FetchMode.Default;
		}

		protected override Dictionary<string, string> GenerateTableAlias(int n, string userAlias, string path, IJoinable joinable)
		{
            var sqlAliases = new Dictionary<string, string>();
			bool shouldCreateUserAlias = joinable.ConsumesEntityAlias(); 
			if(shouldCreateUserAlias == false  && joinable.IsCollection)
			{
				var elementType = ((ICollectionPersister)joinable).ElementType;
				if (elementType != null)
					shouldCreateUserAlias = elementType.IsComponentType;
			}
			if (shouldCreateUserAlias)
			{
                ICriteria[] subcriteria = translator.GetCriteria(userAlias, path);
			    foreach (var criteria in subcriteria)
			    {
                    string sqlAlias = criteria == null ? null : translator.GetSQLAlias(criteria);
			        userAliasList.Add(sqlAlias != null ? criteria.Alias : null);
                    if (sqlAlias != null)
                    {
                        sqlAliases.Add(sqlAlias, criteria.Alias);
                    }
                    else
                    {
                        sqlAliases.Add(StringHelper.GenerateAlias(joinable.Name, n + translator.SQLAliasCount), string.Empty);
                    }
			    }
			}
			else
			{
                sqlAliases.Add(StringHelper.GenerateAlias(joinable.Name, n + translator.SQLAliasCount), string.Empty);
			}
            return sqlAliases;
		}

		protected override string GenerateRootAlias(string tableName)
		{
			return CriteriaQueryTranslator.RootSqlAlias;
			// NH: really not used (we are using a different ctor to support SubQueryCriteria)
		}

		protected override SqlString GetWithClause(string alias, string path)
		{
            return translator.GetWithClause(alias, path, EnabledFilters);
		}
	}
}