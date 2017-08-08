package aspen;


import java.io.ByteArrayInputStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.fsc.core.framework.persistence.SubQuery;
import com.follett.fsc.core.k12.beans.DataAudit;
import com.follett.fsc.core.k12.beans.DistrictSchoolYearContext;
import com.follett.fsc.core.k12.beans.ReferenceCode;
import com.follett.fsc.core.k12.beans.ReferenceTable;
import com.follett.fsc.core.k12.beans.Report;
import com.follett.fsc.core.k12.beans.School;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.tools.ReferenceDescriptionLookup;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.tools.reports.ReportUtils;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.sis.model.beans.ConductAction;
import com.x2dev.sis.model.beans.ConductIncident;
import com.x2dev.sis.model.beans.SisSchool;
import com.x2dev.sis.model.beans.SisStudent;
import com.x2dev.sis.model.beans.StudentAttendance;
import com.x2dev.sis.model.beans.StudentEnrollment;
import com.x2dev.utils.types.PlainDate;
import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;


public class RPS_STATE_REPORTING_INCIDENT_V2 extends ReportJavaSourceNet
{
	private static final String							STATUS_ACTIVE 					= "Active";
	private static final String							QUERY_BY = "queryBy";
	private static final String							DISTRICT_PARAM 					= "districtSummary"; 

	/**
	 *	Start Date Parameter. 
	 */	
	private static final String							START_DATE						= "StartDate"; 

	/**
	 *	End Date Parameter.
	 */	
	private static final String							END_DATE						= "EndDate"; 

	/**
	 *	End Date Parameter.
	 */	
	private static final String							FIELD_SCHOOL					= "School";

	private static final String							CONDUCT_INCIDENT_REF_CODE = "Conduct Incident";

	private static final String							DISTRICT_SUMMARY				="District Summary";
	/**
	 *	All students Incident Statistic Fields
	 */	
	private static final String							FIELD_A1						= "A1";
	private static final String							FIELD_A2						= "A2";
	private static final String							FIELD_A3						= "A3";
	private static final String							FIELD_A4						= "A4";
	private static final String							FIELD_A5						= "A5";
	private static final String							FIELD_A6						= "A6";
	private static final String							FIELD_A7						= "A7";
	private static final String							FIELD_A8						= "A8";
	private static final String							FIELD_A9						= "A9";
	private static final String							FIELD_A10						= "A10";
	private static final String							FIELD_A11						= "A11";
	private static final String							FIELD_A12						= "A12";
	private static final String							FIELD_A13						= "A13";
	private static final String							FIELD_A14						= "A14";
	private static final String							FIELD_A15						= "A15";
	private static final String							FIELD_A16						= "A16";
	private static final String							FIELD_A17						= "A17";
	private static final String							FIELD_A18						= "A18";
	private static final String							FIELD_A19						= "A19";

	/**
	 *	Disabled students Incident Statistics
	 */	
	private static final String							FIELD_D1						= "D1";
	private static final String							FIELD_D2						= "D2";
	private static final String							FIELD_D3						= "D3";
	private static final String							FIELD_D4						= "D4";
	private static final String							FIELD_D5						= "D5";
	private static final String							FIELD_D6						= "D6";
	private static final String							FIELD_D7						= "D7";
	private static final String							FIELD_D8						= "D8";
	private static final String							FIELD_D9						= "D9";
	private static final String							FIELD_D10						= "D10";
	private static final String							FIELD_D11						= "D11";
	private static final String							FIELD_D12						= "D12";
	private static final String							FIELD_D13						= "D13";
	private static final String							FIELD_D14						= "D14";
	private static final String							FIELD_D15						= "D15";
	private static final String							FIELD_D16						= "D16";
	private static final String							FIELD_D17						= "D17";
	private static final String							FIELD_D18						= "D18";
	private static final String							FIELD_D19						= "D19";

	/**
	 *	LEP students Incident Statistics
	 */	
	private static final String							FIELD_E1						= "E1";
	private static final String							FIELD_E2						= "E2";
	private static final String							FIELD_E3						= "E3";
	private static final String							FIELD_E4						= "E4";
	private static final String							FIELD_E5						= "E5";
	private static final String							FIELD_E6						= "E6";
	private static final String							FIELD_E7						= "E7";
	private static final String							FIELD_E8						= "E8";
	private static final String							FIELD_E9						= "E9";
	private static final String							FIELD_E10						= "E10";
	private static final String							FIELD_E11						= "E11";
	private static final String							FIELD_E12						= "E12";
	private static final String							FIELD_E13						= "E13";
	private static final String							FIELD_E14						= "E14";
	private static final String							FIELD_E15						= "E15";
	private static final String							FIELD_E16						= "E16";
	private static final String							FIELD_E17						= "E17";
	private static final String							FIELD_E18						= "E18";
	private static final String							FIELD_E19						= "E19";
	
	/** Detail report fields
	 * 
	 */
	private static final String							FIELD_INCIDENT_ID				= "IncidentID"; 
	private static final String							FIELD_STUDENT_COUNT				= "NumStudent";
	private static final String							FIELD_INCIDENT_DATE				= "IncidentDate";
	private static final String							FIELD_CODE						= "Code"; 
	
	
	
	private static final String							FIELD_SORT						= "SORT COLUMN";
	private static final String							SUB_SUM							="SubSum";
	private static final String							SUB_DETAIL						="SubDetail";
	private static final String							GRID_SUM						="GridSum";
	private static final String							GRID_DETAIL						="GridDetail";
	
	private static final String 						SUB_ID_SUM						= "RPS_SQRI_SUB1";
	private static final String 						SUB_ID_DETAIL					= "RPS_SQRI_SUB2";

	private PlainDate m_YearStartDate;
	private Map<String, HashMap<String, Collection<ConductIncident>>> m_incidents;
	private Map<String, HashMap<String, Collection<ConductIncident>>> m_incidents2;
	private Map<String, HashMap<String, Collection<ConductIncident>>> m_incidents3;
	private Map<String, Collection<ConductAction>> m_actions;
	private Map<String, Collection<Infraction >> m_schoolInfractions;
	private Collection<Infraction >  m_infractions;
	private PlainDate m_endDate;
	private PlainDate m_startDate;
	private Map <String, SisStudent> m_students;
	private SisSchool m_currentSchool;
	private SubQuery m_schoolSub;
	private Map<String, ReferenceCode> m_refCodes;
	private boolean m_districtSummary;
	private DistrictSchoolYearContext m_currentYear;
	private Report SumReport;
	private Report DetailReport;

	@Override
	protected Object gatherData() throws Exception {
		ReportDataGrid grid = new ReportDataGrid();
 
		m_startDate =(PlainDate)getParameter(START_DATE);
		m_endDate =(PlainDate)getParameter(END_DATE);
		m_districtSummary = false;
		getSchoolYear();
		boolean detail = (boolean) getParameter("Detail");
		SumReport = ReportUtils.getReport(SUB_ID_SUM, getBroker());
		DetailReport = ReportUtils.getReport(SUB_ID_DETAIL, getBroker());
		
		m_YearStartDate = m_currentYear.getStartDate();
		if (m_endDate.compareTo(m_currentYear.getEndDate())>0 ) m_endDate = m_currentYear.getEndDate();
		
		if (!isSchoolContext())
		{
		m_districtSummary = (boolean)getParameter(DISTRICT_PARAM);
		}

		m_schoolSub = getSchoolSubQuery();

		if (m_endDate.compareTo(m_startDate) < 0)
		{
			m_endDate = m_startDate;
		}
		addParameter(START_DATE,m_startDate );
		addParameter(END_DATE,m_endDate );
		addParameter("Detail",detail );
		
		LoadIncidents();
		loadIncidentStudents();
		loadReference();
		ProcessIncidents();
		ProcessStats( grid);
		grid.beforeTop();
		grid.sort(FIELD_SCHOOL, false);
		return grid;
	}

	public void getSchoolYear()
	{
		Criteria criteria = new Criteria();
		criteria.addGreaterOrEqualThan(DistrictSchoolYearContext.COL_END_DATE, m_startDate);
		criteria.addLessOrEqualThan(DistrictSchoolYearContext.COL_START_DATE, m_startDate);
		QueryByCriteria query =  new QueryByCriteria(DistrictSchoolYearContext.class, criteria);
		m_currentYear = (DistrictSchoolYearContext) getBroker().getBeanByQuery(query); 
	}

	
	@SuppressWarnings("unchecked")
	private void LoadIncidents()
	{
		/// Get all Primary Incident Codes
		Criteria criteria = new Criteria();
		Criteria criteriaA = new Criteria();
		Criteria criteriaB = new Criteria();
		Criteria comboCriteria = new Criteria();
	 	criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_YearStartDate);
		criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_startDate);
		criteria.addLessOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_endDate);
		criteria.addIn(ConductIncident.COL_SCHOOL_OID , m_schoolSub);
		
		
		criteriaA.addEqualTo(ConductIncident.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_FIELD_A032 , "0"); 
		criteriaB.addEqualTo(ConductIncident.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_FIELD_A032 , null); 
		comboCriteria.addOrCriteria(criteriaA);
		comboCriteria.addOrCriteria(criteriaB);
		criteria.addAndCriteria(comboCriteria); 
		
		
		QueryByCriteria query = new QueryByCriteria(ConductIncident.class, criteria);

		String[] columns = 	{
				ConductIncident.COL_INCIDENT_ID, 
				ConductIncident.COL_INCIDENT_CODE
		};
		int[] Sizes = {1000,4};
		m_incidents = getBroker().getGroupedCollectionByQuery(query, columns, Sizes);



		// Get All Actions
		SubQuery IncidentSub = new SubQuery(ConductIncident.class, ConductIncident.COL_OID, criteria );
		Criteria actionCriteria = new Criteria();
		actionCriteria.addIn(ConductAction.COL_INCIDENT_OID, IncidentSub);
		QueryByCriteria ActionQuery = new QueryByCriteria(ConductAction.class, actionCriteria);


		m_actions =	getBroker().getGroupedCollectionByQuery(ActionQuery  , ConductAction.COL_INCIDENT_OID , 1000);

		/// Get all Primary Incident Codes
		criteria = new Criteria();
	 	criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_YearStartDate);
		criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_startDate);
		criteria.addLessOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_endDate);
		criteria.addIn(ConductIncident.COL_SCHOOL_OID , m_schoolSub); 
		criteria.addNotNull(ConductIncident.COL_FIELD_A008 );
		criteria.addAndCriteria(comboCriteria); 

		query = new QueryByCriteria(ConductIncident.class, criteria);

		columns = 	new String[]{
				ConductIncident.COL_INCIDENT_ID, 
				ConductIncident.COL_FIELD_A008
		};

		m_incidents2 = getBroker().getGroupedCollectionByQuery(query, columns, Sizes);

		/// Get all Primary Incident Codes
		criteria = new Criteria();
		 criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_YearStartDate);
	 	criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_startDate);
		criteria.addLessOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_endDate);
		criteria.addIn(ConductIncident.COL_SCHOOL_OID , m_schoolSub); 
		criteria.addNotNull(ConductIncident.COL_FIELD_A009 );
		criteria.addAndCriteria(comboCriteria); 

		query = new QueryByCriteria(ConductIncident.class, criteria);

		columns = new String[] 	{
				ConductIncident.COL_INCIDENT_ID, 
				ConductIncident.COL_FIELD_A009
		}; 
		m_incidents3 = getBroker().getGroupedCollectionByQuery(query, columns, Sizes);


	}

	@SuppressWarnings("unchecked")
	private void loadIncidentStudents()
	{
		Criteria criteria = new Criteria();
		Criteria criteriaA = new Criteria();
		Criteria criteriaB = new Criteria();
		Criteria comboCriteria = new Criteria();
		
	 	criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_YearStartDate);
		criteria.addGreaterOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_startDate);
		criteria.addLessOrEqualThan(ConductIncident.COL_INCIDENT_DATE , m_endDate);
		criteria.addIn(ConductIncident.COL_SCHOOL_OID ,m_schoolSub);  
		SubQuery subquery = new SubQuery(ConductIncident.class,ConductIncident.COL_STUDENT_OID , criteria);
		subquery.setDistinct(true); 

		Criteria StuCriteria = new Criteria();
		criteriaA.addEqualTo( SisStudent.COL_FIELD_A032 , "0"); 
		criteriaB.addEqualTo( SisStudent.COL_FIELD_A032 , null); 
		comboCriteria.addOrCriteria(criteriaA);
		comboCriteria.addOrCriteria(criteriaB);
		StuCriteria.addAndCriteria(comboCriteria); 
		StuCriteria.addIn(SisStudent.COL_OID, subquery);
		QueryByCriteria stuQuery = new QueryByCriteria(SisStudent.class, StuCriteria);
		m_students = getBroker().getMapByQuery(stuQuery, SisStudent.COL_OID, 1000); 
		
	}


	/**
	 * Returns a sub query for the list of Schools to include in the query.
	 * 
	 * @param beanPath
	 * 
	 * @return SubQuery;
	 */
	private SubQuery getSchoolSubQuery( )
	{ 

		Criteria subCriteria = new Criteria(); 
		
		

		if (!m_districtSummary && isSchoolContext()){
			subCriteria.addEqualTo(SisSchool.COL_OID, getSchool().getOid());
		}
		else if (!m_districtSummary && m_currentSchool != null )
		{
			subCriteria.addEqualTo(SisSchool.COL_OID, m_currentSchool.getOid());
		} 
		else 
		{
			String queryString=  (String) getParameter(QUERY_BY);  

			if (!m_districtSummary && queryString!= null && queryString.equals("##current"))
			{ 
				Criteria criteria = new Criteria();
				criteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
				
				subCriteria.addAndCriteria(getCurrentCriteria());
				subCriteria.addAndCriteria(criteria);
			}
			else
			{
				subCriteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
			}
		}

		SubQuery schoolSubQuery = new SubQuery(SisSchool.class, X2BaseBean.COL_OID, subCriteria); 
		return schoolSubQuery;

	}

	/**
	 * @see com.x2dev.sis.tools.ToolJavaSource#saveState(com.x2dev.sis.web.UserDataContainer)
	 */
	@Override
	protected void saveState(final UserDataContainer userData)
	{
		/*
		 * If we're in the context of a single student, print the report for just that student
		 */
		m_currentSchool = (SisSchool) userData.getCurrentRecord(SisSchool.class);
	}


	private void loadReference()
	{
		Criteria criteria = new Criteria();
		criteria.addEqualTo(ReferenceCode.REL_REFERENCE_TABLE + PATH_DELIMITER + ReferenceTable.COL_USER_NAME, CONDUCT_INCIDENT_REF_CODE );
		QueryByCriteria query = new  QueryByCriteria(ReferenceCode.class, criteria);
		m_refCodes = getBroker().getMapByQuery(query, ReferenceCode.COL_CODE, 100);

	}


	private void ProcessIncidents()
	{

		m_schoolInfractions = new HashMap<String, Collection<Infraction >>();
		m_infractions = new HashSet<Infraction >();

		Collection<String> IncidentIDs = 	m_incidents.keySet();

		for (String IncidentID : IncidentIDs)
		{
			Map<String , Collection <ConductIncident>>  IncidentCodeLevel =   m_incidents.get(IncidentID);
			Collection<String> IncidentCodes = 	IncidentCodeLevel.keySet();
			for (String IncidentCode : IncidentCodes)
			{
				if (m_refCodes.containsKey((IncidentCode)))
						{
				Collection <ConductIncident> incidents = IncidentCodeLevel.get(IncidentCode);
				String StateCode = m_refCodes.get(IncidentCode).getStateCode();
				Infraction infraction = new Infraction(IncidentID, IncidentCode,StateCode, incidents);
				infraction.PairActions(m_actions);
				infraction.PairStudent(m_students);

				m_infractions.add(infraction);

				if (!m_schoolInfractions.containsKey(infraction.SchoolOid))
				{
					Collection<Infraction> infractions = new HashSet<Infraction>();
					infractions.add(infraction);
					m_schoolInfractions.put(infraction.SchoolOid, infractions);
				}
				else
				{
					m_schoolInfractions.get(infraction.SchoolOid).add(infraction);
				}
						}
			}	
		}

		//  ADD Secondary Infractions
		if (m_incidents2 !=null)
		{
			IncidentIDs = 	m_incidents2.keySet();

			for (String IncidentID : IncidentIDs)
			{
				Map<String , Collection <ConductIncident>>  IncidentCodeLevel =   m_incidents2.get(IncidentID);
				Collection<String> IncidentCodes = 	IncidentCodeLevel.keySet();
				for (String IncidentCode : IncidentCodes)
				{if (m_refCodes.containsKey((IncidentCode)))
				{
					Collection <ConductIncident> incidents = IncidentCodeLevel.get(IncidentCode);
					String StateCode = m_refCodes.get(IncidentCode).getStateCode();
					Infraction infraction = new Infraction(IncidentID, IncidentCode,StateCode,  incidents);
					infraction.PairActions(m_actions);
					infraction.PairStudent(m_students);

					m_infractions.add(infraction);

					if (!m_schoolInfractions.containsKey(infraction.SchoolOid))
					{
						Collection<Infraction> infractions = new HashSet<Infraction>();
						infractions.add(infraction);
						m_schoolInfractions.put(infraction.SchoolOid, infractions);
					}
					else
					{
						m_schoolInfractions.get(infraction.SchoolOid).add(infraction);
					}
				}
				}	
			}
		}
		//  ADD Third Infractions
		if (m_incidents3 !=null)
		{
			IncidentIDs = 	m_incidents3.keySet();

			for (String IncidentID : IncidentIDs)
			{
				Map<String , Collection <ConductIncident>>  IncidentCodeLevel =   m_incidents3.get(IncidentID);
				Collection<String> IncidentCodes = 	IncidentCodeLevel.keySet();
				for (String IncidentCode : IncidentCodes)
				{if (m_refCodes.containsKey((IncidentCode)))
				{
					Collection <ConductIncident> incidents = IncidentCodeLevel.get(IncidentCode);
					String StateCode = m_refCodes.get(IncidentCode).getStateCode();
					Infraction infraction = new Infraction(IncidentID, IncidentCode,StateCode, incidents);
					infraction.PairActions(m_actions);
					infraction.PairStudent(m_students);

					m_infractions.add(infraction);

					if (!m_schoolInfractions.containsKey(infraction.SchoolOid))
					{
						Collection<Infraction> infractions = new HashSet<Infraction>();
						infractions.add(infraction);
						m_schoolInfractions.put(infraction.SchoolOid, infractions);
					}
					else
					{
						m_schoolInfractions.get(infraction.SchoolOid).add(infraction);
					}

				}	}
			}	
		}

	}

	private void ProcessStats(final ReportDataGrid grid)
	{
		Collection<String> schoolOids = m_schoolInfractions.keySet();
		if (!m_districtSummary )
		{	
		for( String schoolOid : schoolOids)
		{
			if (m_schoolInfractions.containsKey(schoolOid))
			{	
				ReportDataGrid SumGrid = new ReportDataGrid();
				ReportDataGrid DetailGrid = new ReportDataGrid();
				grid.append();
				IncidentStatistics AllStudent = new IncidentStatistics( schoolOid, m_startDate, m_endDate);
				IncidentStatistics Disabled	  = new IncidentStatistics( schoolOid, m_startDate, m_endDate);
				IncidentStatistics ELL 		  = new IncidentStatistics( schoolOid, m_startDate, m_endDate);

				SisSchool school  = (SisSchool) getBroker().getBeanByOid(SisSchool.class, schoolOid);
				grid.set(FIELD_SCHOOL, school.getName());
				Collection<Infraction> Incidents = m_schoolInfractions.get(schoolOid);

				for (Infraction incident: Incidents)
				{
					if (incident.ReportThisIncident())
					{
						AllStudent.processIncident(incident.StateCode); 
						DetailGrid.append();
						DetailGrid.set(FIELD_INCIDENT_ID, incident.IncidentID);
						DetailGrid.set(FIELD_INCIDENT_DATE, incident.IncidentDate); 
						DetailGrid.set(FIELD_CODE, incident.StateCode);  
						DetailGrid.set(FIELD_STUDENT_COUNT, incident.Students.size());
						DetailGrid.set(FIELD_SORT, incident.StateCode + incident.IncidentDate.toString());

						// LEP Student
						if (incident.ReportThisIncidentLEP())
						{
							ELL.processIncident(incident.StateCode);

						}
						// Students with Disabilities
						if (incident.ReportThisIncidentDis())
						{
							Disabled.processIncident(incident.StateCode);
						}
					}				
				}
				
				SumGrid.append();
				SumGrid.set(FIELD_SCHOOL, (String) school.getName());
				SumGrid.set(FIELD_A1	, AllStudent.Weapon       );
				SumGrid.set(FIELD_A2	, AllStudent.OAStudents   );
				SumGrid.set(FIELD_A3	, AllStudent.OAStaff      );
				SumGrid.set(FIELD_A4	, AllStudent.Malicious    );
				SumGrid.set(FIELD_A5	, AllStudent.Alcohol      );
				SumGrid.set(FIELD_A6	, AllStudent.Gang         );
				SumGrid.set(FIELD_A7	, AllStudent.Harrassment  );
				SumGrid.set(FIELD_A8	, AllStudent.Disorderly   );
				SumGrid.set(FIELD_A9	, AllStudent.Threat       );
				SumGrid.set(FIELD_A10	, AllStudent.Arson        );
				SumGrid.set(FIELD_A11	, AllStudent.Attendance   );
				SumGrid.set(FIELD_A12	, AllStudent.Breaking     );
				SumGrid.set(FIELD_A13	, AllStudent.Electronic   );
				SumGrid.set(FIELD_A14	, AllStudent.Extorsion    );
				SumGrid.set(FIELD_A15	, AllStudent.Riot         );
				SumGrid.set(FIELD_A16	, AllStudent.Homicide     );
				SumGrid.set(FIELD_A17	, AllStudent.Other        );
				SumGrid.set(FIELD_A18	, AllStudent.Fighting     );
				SumGrid.set(FIELD_A19	, AllStudent.Sexual 	  );
                 
				SumGrid.set(FIELD_D1	, Disabled.Weapon       );
				SumGrid.set(FIELD_D2	, Disabled.OAStudents   );
				SumGrid.set(FIELD_D3	, Disabled.OAStaff      );
				SumGrid.set(FIELD_D4	, Disabled.Malicious    );
				SumGrid.set(FIELD_D5	, Disabled.Alcohol      );
				SumGrid.set(FIELD_D6	, Disabled.Gang         );
				SumGrid.set(FIELD_D7	, Disabled.Harrassment  );
				SumGrid.set(FIELD_D8	, Disabled.Disorderly   );
				SumGrid.set(FIELD_D9	, Disabled.Threat       );
				SumGrid.set(FIELD_D10	, Disabled.Arson        );
				SumGrid.set(FIELD_D11	, Disabled.Attendance   );
				SumGrid.set(FIELD_D12	, Disabled.Breaking     );
				SumGrid.set(FIELD_D13	, Disabled.Electronic   );
				SumGrid.set(FIELD_D14	, Disabled.Extorsion    );
				SumGrid.set(FIELD_D15	, Disabled.Riot         );
				SumGrid.set(FIELD_D16	, Disabled.Homicide     );
				SumGrid.set(FIELD_D17	, Disabled.Other        );
				SumGrid.set(FIELD_D18	, Disabled.Fighting     );
				SumGrid.set(FIELD_D19	, Disabled.Sexual 	    );
                 
				SumGrid.set(FIELD_E1	, ELL.Weapon       );
				SumGrid.set(FIELD_E2	, ELL.OAStudents   );
				SumGrid.set(FIELD_E3	, ELL.OAStaff      );
				SumGrid.set(FIELD_E4	, ELL.Malicious    );
				SumGrid.set(FIELD_E5	, ELL.Alcohol      );
				SumGrid.set(FIELD_E6	, ELL.Gang         );
				SumGrid.set(FIELD_E7	, ELL.Harrassment  );
				SumGrid.set(FIELD_E8	, ELL.Disorderly   );
				SumGrid.set(FIELD_E9	, ELL.Threat       );
				SumGrid.set(FIELD_E10	, ELL.Arson        );
				SumGrid.set(FIELD_E11	, ELL.Attendance   );
				SumGrid.set(FIELD_E12	, ELL.Breaking     );
				SumGrid.set(FIELD_E13	, ELL.Electronic   );
				SumGrid.set(FIELD_E14	, ELL.Extorsion    );
				SumGrid.set(FIELD_E15	, ELL.Riot         );
				SumGrid.set(FIELD_E16	, ELL.Homicide     );
				SumGrid.set(FIELD_E17	, ELL.Other        );
				SumGrid.set(FIELD_E18	, ELL.Fighting     );
				SumGrid.set(FIELD_E19	, ELL.Sexual       );
				
				SumGrid.beforeTop();
		DetailGrid.beforeTop();
		DetailGrid.sort(FIELD_SORT, false);
		grid.set(GRID_SUM, SumGrid);
		grid.set(GRID_DETAIL, DetailGrid); 
		grid.set(SUB_SUM,   new ByteArrayInputStream(SumReport.getCompiledFormat()));
		grid.set(SUB_DETAIL,   new ByteArrayInputStream(DetailReport.getCompiledFormat()));  
			}
		}
		
		
		
		}
		else
		{
			grid.append();
			grid.set(FIELD_SCHOOL, DISTRICT_SUMMARY);
			ReportDataGrid SumGrid = new ReportDataGrid();
			ReportDataGrid DetailGrid = new ReportDataGrid();
			IncidentStatistics AllStudent = new IncidentStatistics( "District", m_startDate, m_endDate);
			IncidentStatistics Disabled	  = new IncidentStatistics( "District", m_startDate, m_endDate);
			IncidentStatistics ELL 		  = new IncidentStatistics( "District", m_startDate, m_endDate);

			Collection<Infraction> Incidents = m_infractions;

			for (Infraction incident: Incidents)
			{
				if (incident.ReportThisIncident())
				{
					DetailGrid.append();
					DetailGrid.set(FIELD_INCIDENT_ID, incident.IncidentID);
					DetailGrid.set(FIELD_INCIDENT_DATE, incident.IncidentDate); 
					DetailGrid.set(FIELD_CODE, incident.StateCode);  
					DetailGrid.set(FIELD_STUDENT_COUNT, incident.Students.size());
					DetailGrid.set(FIELD_SORT, incident.StateCode + incident.IncidentDate.toString());
					AllStudent.processIncident(incident.StateCode); 

					// LEP Student
					if (incident.ReportThisIncidentLEP())
					{
						ELL.processIncident(incident.StateCode);

					}
					// Students with Disabilities
					if (incident.ReportThisIncidentDis())
					{
						Disabled.processIncident(incident.StateCode);
					}
				}				
			}
			
			SumGrid.append(); 
			SumGrid.set(FIELD_SCHOOL, DISTRICT_SUMMARY);
			SumGrid.set(FIELD_A1	, AllStudent.Weapon       );
			SumGrid.set(FIELD_A2	, AllStudent.OAStudents   );
			SumGrid.set(FIELD_A3	, AllStudent.OAStaff      );
			SumGrid.set(FIELD_A4	, AllStudent.Malicious    );
			SumGrid.set(FIELD_A5	, AllStudent.Alcohol      );
			SumGrid.set(FIELD_A6	, AllStudent.Gang         );
			SumGrid.set(FIELD_A7	, AllStudent.Harrassment  );
			SumGrid.set(FIELD_A8	, AllStudent.Disorderly   );
			SumGrid.set(FIELD_A9	, AllStudent.Threat       );
			SumGrid.set(FIELD_A10	, AllStudent.Arson        );
			SumGrid.set(FIELD_A11	, AllStudent.Attendance   );
			SumGrid.set(FIELD_A12	, AllStudent.Breaking     );
			SumGrid.set(FIELD_A13	, AllStudent.Electronic   );
			SumGrid.set(FIELD_A14	, AllStudent.Extorsion    );
			SumGrid.set(FIELD_A15	, AllStudent.Riot         );
			SumGrid.set(FIELD_A16	, AllStudent.Homicide     );
			SumGrid.set(FIELD_A17	, AllStudent.Other        );
			SumGrid.set(FIELD_A18	, AllStudent.Fighting     );
			SumGrid.set(FIELD_A19	, AllStudent.Sexual 	  );
             
			SumGrid.set(FIELD_D1	, Disabled.Weapon       );
			SumGrid.set(FIELD_D2	, Disabled.OAStudents   );
			SumGrid.set(FIELD_D3	, Disabled.OAStaff      );
			SumGrid.set(FIELD_D4	, Disabled.Malicious    );
			SumGrid.set(FIELD_D5	, Disabled.Alcohol      );
			SumGrid.set(FIELD_D6	, Disabled.Gang         );
			SumGrid.set(FIELD_D7	, Disabled.Harrassment  );
			SumGrid.set(FIELD_D8	, Disabled.Disorderly   );
			SumGrid.set(FIELD_D9	, Disabled.Threat       );
			SumGrid.set(FIELD_D10	, Disabled.Arson        );
			SumGrid.set(FIELD_D11	, Disabled.Attendance   );
			SumGrid.set(FIELD_D12	, Disabled.Breaking     );
			SumGrid.set(FIELD_D13	, Disabled.Electronic   );
			SumGrid.set(FIELD_D14	, Disabled.Extorsion    );
			SumGrid.set(FIELD_D15	, Disabled.Riot         );
			SumGrid.set(FIELD_D16	, Disabled.Homicide     );
			SumGrid.set(FIELD_D17	, Disabled.Other        );
			SumGrid.set(FIELD_D18	, Disabled.Fighting     );
			SumGrid.set(FIELD_D19	, Disabled.Sexual 	    );
             
			SumGrid.set(FIELD_E1	, ELL.Weapon       );
			SumGrid.set(FIELD_E2	, ELL.OAStudents   );
			SumGrid.set(FIELD_E3	, ELL.OAStaff      );
			SumGrid.set(FIELD_E4	, ELL.Malicious    );
			SumGrid.set(FIELD_E5	, ELL.Alcohol      );
			SumGrid.set(FIELD_E6	, ELL.Gang         );
			SumGrid.set(FIELD_E7	, ELL.Harrassment  );
			SumGrid.set(FIELD_E8	, ELL.Disorderly   );
			SumGrid.set(FIELD_E9	, ELL.Threat       );
			SumGrid.set(FIELD_E10	, ELL.Arson        );
			SumGrid.set(FIELD_E11	, ELL.Attendance   );
			SumGrid.set(FIELD_E12	, ELL.Breaking     );
			SumGrid.set(FIELD_E13	, ELL.Electronic   );
			SumGrid.set(FIELD_E14	, ELL.Extorsion    );
			SumGrid.set(FIELD_E15	, ELL.Riot         );
			SumGrid.set(FIELD_E16	, ELL.Homicide     );
			SumGrid.set(FIELD_E17	, ELL.Other        );
			SumGrid.set(FIELD_E18	, ELL.Fighting     );
			SumGrid.set(FIELD_E19	, ELL.Sexual       );
			
			SumGrid.beforeTop();
			DetailGrid.beforeTop();
			DetailGrid.sort(FIELD_SORT, false);
			grid.set(GRID_SUM, SumGrid);
			grid.set(GRID_DETAIL, DetailGrid); 
			grid.set(SUB_SUM,   new ByteArrayInputStream(SumReport.getCompiledFormat()));
			grid.set(SUB_DETAIL,   new ByteArrayInputStream(DetailReport.getCompiledFormat()));  
		}
	};

	public class Infraction
	{
		private final String[] MustReportCodes = {"AL1","BA1","BA2","BA3","BA4","BA5","BB1","BO1","BO3","BO4","BU1","BU2","D17","DG7","DR1","DR2",
				"DR3","DR4","DR5","D15","D16","FA2","HR1","HO1","HO2","HO3","HO4","KI1","RO1","RB1","SB1","SB2","ST1","SX0","SX1","SX2","SX3","SX4","SX5","SX6","SX7","SX8","TC1","TC2","TB1","TB2",
				"TI1","TI2","W2P","WP0","WP1","WP2","WP4","WP5","WP6","WP7","WP8","WP9","WS1","WT1"};
		private final String[] BasedOnSactionCodes = {"A1T","AR1","AS1","AS2","AS3","BR1","C1M","C2M","C3M","D1C","D2C","D3C","D4C","D4G","D5C","D5G",
				"D6C","D6G","D8C", "EX1","F1T","G1B","GA1","H1Z","RT1","S1V","S2V","S3V","T1C",
				"T2C","T3C","T4B","T4C","TF1","TF2","TF3","TF4","TH1","TH2","TR1","VA1","W1P" };
		private final String[] SanctionCodes = {"OSS","PANPEN", "SUSCNF", "OSS03", "Expelled", "OSS07"  } ;
		private List<String> MustReport     ;
		private List<String> BasedOnSaction ;
		private List<String> Sanctions ;
		public String IncidentCode;
		public String IncidentID;
		public String StateCode;
		public Collection < ConductIncident> Incidents;
		public Collection <String> IncidentOIDs;
		public Map <String,  ConductAction> Actions;
		public Map <String, SisStudent> Students; 
		public String SchoolOid;
		public PlainDate IncidentDate;

		public Infraction (String ID, String Code,String State,   Collection<ConductIncident> Inc )
		{
			IncidentCode = Code;
			IncidentID = ID;
			Incidents = Inc; 
			IncidentDate = Inc.iterator().next().getIncidentDate();
			Actions = new HashMap<String, ConductAction>();
			Students = new HashMap <String, SisStudent>();		
			IncidentOIDs = new HashSet<String>();
			StateCode =State;
			if (Incidents != null)
			{
				for (ConductIncident ci :Incidents)
				{
					IncidentOIDs.add(ci.getOid());
					SchoolOid = ci.getSchoolOid(); 
				}
			}

			MustReport = new ArrayList<String>(Arrays.asList(MustReportCodes));
			BasedOnSaction= new ArrayList<String>(Arrays.asList(BasedOnSactionCodes));
			Sanctions = new ArrayList<String>(Arrays.asList(SanctionCodes));
		}

		public void PairActions( Map<String , Collection<ConductAction>> Acts )
		{
			for (String OID :IncidentOIDs )
			{

				if  (Acts!=null && Acts.containsKey(OID))
				{	
					Collection <ConductAction> CActs = Acts.get(OID)	;
					for (ConductAction action : CActs)
					{
						Actions.put(OID, action);
					}
				}
			}
		} 


		public void PairStudent( Map<String ,  SisStudent> Stu )
		{

			Collection <ConductAction> Acts = Actions.values();

			for (ConductAction CA : Acts )
			{  
				SisStudent student = m_students.get( CA.getStudentOid());
				if (student != null && Students != null)
				{
					Students.put(student.getOid(), student);
				}
			}
		}

		public boolean ReportThisIncident()
		{
			if (MustReport.contains(StateCode))
			{
				return true;
			}

			if (BasedOnSaction.contains(StateCode))
			{
				Collection<ConductAction> actions= Actions.values();
				for (ConductAction action : actions)
				{
					if ( Sanctions.contains( action.getActionCode()))
					{
						return true;
					}
				}

			}
			return false;
		}

		public boolean ReportThisIncidentDis()
		{
			if (MustReport.contains(StateCode))
			{

				Collection <SisStudent> StudentCol = Students.values();
				for (SisStudent student : StudentCol)
				{
					if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
					{
						return true;
					}					
				}
			}

			if (BasedOnSaction.contains(StateCode))
			{
				Collection<ConductAction> actions= Actions.values();
				for (ConductAction action : actions)
				{
					if ( Sanctions.contains( action.getActionCode()))
					{if( Students.containsKey(action.getStudentOid()))
					{
						SisStudent student = Students.get(action.getStudentOid());
						if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
						{
							return true;
						}
					}
					}
				}

			}
			return false;
		}

		public boolean ReportThisIncidentLEP()
		{
			if (MustReport.contains(StateCode))
			{

				Collection <SisStudent> StudentCol = Students.values();
				for (SisStudent student : StudentCol)
				{
					if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
					{
						return true;
					}					
				}
			}

			if (BasedOnSaction.contains(StateCode))
			{
				Collection<ConductAction> actions= Actions.values();
				for (ConductAction action : actions)
				{
					if ( Sanctions.contains( action.getActionCode()))
					{
						if( Students.containsKey(action.getStudentOid()))
						{
							SisStudent student = Students.get(action.getStudentOid());
							if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
							{
								return true;
							}
						}
					}
				} 
			}
			return false;
		}
	}

	public class IncidentStatistics
	{
		private final String[] C1	={ "W1P","W2P","W3P","W8P","W9P","WP0","WP1","WP2","WP4","WP5","WP6","WP7","WP8","WP9","WS1","WT1"};
		private final String[] C2	={ "BA3","BA4","BA6" };
		private final String[] C3	={ "BA1","BA2" };
		private final String[] C4	={ "BA5"};
		private final String[] C5	={ "AC1","AC2","AC3","AL1","D10","D11","D12","D15","D16","D17","D19","TB1","D20","TC1","TC2","TC3","T4B","D4G","D5G","D6G","DG1","DG2","DG3","DG5","DG6","DG7","DG8","DG9","DR3" };
		private final String[] C6	={ "GA1"};
		private final String[] C7	={ "H1Z","HR1", "ST1" };
		private final String[] C8	={ "D1C","D20","D2C","D3C","D4C","D4G","D5C","D5G","D6C","D6G","D8C","DC1","DO3","DO7" };
		private final String[] C9	={ "TI1","TI2","BO1","BO2","BO3","BO4","BU1","BU2" };
		private final String[] C10	={ "AR1","AS1","AS2","AS3" };
		private final String[] C11	={ "A1T","ATO02","ATO03","ATO04","ATO05" };
		private final String[] C12	={ "BR1","BK1","BK2","RB1","RB2","RO1","VA1","VN1","VN2","VN3","TH1","TH2","TR1","TF1","TF2","TF3","TF4","TF6"};
		private final String[] C13  ={ "C1M","C2M","C3M","T1C","T2C","T3C","T4B","T4C" };
		private final String[] C14	={ "ET1","ET2","G1B" };
		private final String[] C15	={ "RG1","RG2" };
		private final String[] C16	={ "HO1","HO2","HO3","HO4","KI1" };
		private final String[] C17	={ "S1V","S2V","S3V","OT1" };
		private final String[] C18	={ "F1T","FA2","FA1" };
		private final String[] C19	={ "SB1","SB2","SX0","SX1","SX2","SX3","SX4","SX5","SX6","SX7","SX8" };


		public PlainDate StartDate; 
		public PlainDate EndDate;
		public String  SchoolOid;
		public Integer Weapon;
		public Integer OAStudents;
		public Integer OAStaff;
		public Integer Malicious;
		public Integer Alcohol;
		public Integer Gang;
		public Integer Harrassment;
		public Integer Disorderly;
		public Integer Threat;
		public Integer Arson;
		public Integer Attendance;
		public Integer Breaking;
		public Integer Electronic;
		public Integer Extorsion;
		public Integer Riot;
		public Integer Homicide;
		public Integer Other;
		public Integer Fighting;
		public Integer Sexual;		

		private List<String> WeaponList     ;
		private List<String> OAStudentsList ;
		private List<String> OAStaffList    ;
		private List<String> MaliciousList  ;
		private List<String> AlcoholList    ;
		private List<String> GangList       ;
		private List<String> HarrassmentList;
		private List<String> DisorderlyList ;
		private List<String> ThreatList     ;
		private List<String> ArsonList      ;
		private List<String> AttendanceList ;
		private List<String> BreakingList   ;
		private List<String> ElectronicList ;
		private List<String> ExtorsionList  ;
		private List<String> RiotList       ;
		private List<String> HomicideList   ;
		private List<String> OtherList      ;
		private List<String> FightingList   ;
		private List<String> SexualList	    ;

		public IncidentStatistics( String SOid, PlainDate startDate, PlainDate endDate)
		{
			SchoolOid = SOid; 
			StartDate = startDate;
			EndDate = endDate;
			Weapon = 0;
			OAStudents = 0;
			OAStaff = 0;
			Malicious = 0;
			Alcohol = 0;
			Gang = 0;
			Harrassment = 0;
			Disorderly = 0;
			Threat = 0;
			Arson = 0;
			Attendance = 0;
			Breaking = 0;
			Electronic = 0;
			Extorsion = 0;
			Riot = 0;
			Homicide = 0;
			Other = 0;
			Fighting = 0;
			Sexual = 0; 
			WeaponList      = new ArrayList<String>(Arrays.asList(C1));
			OAStudentsList  = new ArrayList<String>(Arrays.asList(C2));
			OAStaffList     = new ArrayList<String>(Arrays.asList(C3));
			MaliciousList   = new ArrayList<String>(Arrays.asList(C4));
			AlcoholList     = new ArrayList<String>(Arrays.asList(C5));
			GangList        = new ArrayList<String>(Arrays.asList(C6));
			HarrassmentList = new ArrayList<String>(Arrays.asList(C7));
			DisorderlyList  = new ArrayList<String>(Arrays.asList(C8));
			ThreatList      = new ArrayList<String>(Arrays.asList(C9));
			ArsonList       = new ArrayList<String>(Arrays.asList(C10));
			AttendanceList  = new ArrayList<String>(Arrays.asList(C11));
			BreakingList    = new ArrayList<String>(Arrays.asList(C12));
			ElectronicList  = new ArrayList<String>(Arrays.asList(C13));
			ExtorsionList   = new ArrayList<String>(Arrays.asList(C14));
			RiotList        = new ArrayList<String>(Arrays.asList(C15));
			HomicideList    = new ArrayList<String>(Arrays.asList(C16));
			OtherList       = new ArrayList<String>(Arrays.asList(C17));
			FightingList    = new ArrayList<String>(Arrays.asList(C18));
			SexualList	     = new ArrayList<String>(Arrays.asList(C19));
		}

		public void processIncident(String IncidentCode)
		{

			if (WeaponList.contains(IncidentCode)) 			{  Weapon     = Weapon     +1;}
			else if (OAStudentsList.contains(IncidentCode)) {  OAStudents = OAStudents +1;}
			else if (OAStaffList.contains(IncidentCode)) 	{  OAStaff    = OAStaff    +1;}
			else if (MaliciousList.contains(IncidentCode)) 	{  Malicious  = Malicious  +1;}
			else if (AlcoholList.contains(IncidentCode)) 	{  Alcohol    = Alcohol    +1;}
			else if (GangList.contains(IncidentCode)) 		{  Gang       = Gang       +1;}
			else if (HarrassmentList.contains(IncidentCode)){  Harrassment= Harrassment+1;}
			else if (DisorderlyList.contains(IncidentCode)) {  Disorderly = Disorderly +1;}
			else if (ThreatList.contains(IncidentCode)) 	{  Threat     = Threat     +1;}
			else if (ArsonList.contains(IncidentCode)) 		{  Arson      = Arson      +1;}
			else if (AttendanceList.contains(IncidentCode)) {  Attendance = Attendance +1;}
			else if (BreakingList.contains(IncidentCode)) 	{  Breaking   = Breaking   +1;}
			else if (ElectronicList.contains(IncidentCode)) {  Electronic = Electronic +1;}
			else if (ExtorsionList.contains(IncidentCode)) 	{  Extorsion  = Extorsion  +1;}
			else if (RiotList.contains(IncidentCode)) 		{  Riot       = Riot       +1;}
			else if (HomicideList.contains(IncidentCode)) 	{  Homicide   = Homicide   +1;}
			else if (OtherList.contains(IncidentCode)) 		{  Other      = Other      +1;}
			else if (FightingList.contains(IncidentCode)) 	{  Fighting   = Fighting   +1;}
			else if (SexualList.contains(IncidentCode)) 	{  Sexual	  = Sexual	   +1;}

		}

	}
}