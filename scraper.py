import csv

import requests
from bs4 import BeautifulSoup

# Using Benson's code to get couse data for each term


def fetch_prerequisites(session: requests.Session, term: str, course_reference_number: str) -> str:
    """
    Fetch prerequisite information for a specific course.
    
    Args:
        session: Active requests session with UCR authentication
        term: Term code (e.g., "202440" for Fall 2024)
        course_reference_number: Course reference number (CRN)
    
    Returns:
        str: Prerequisite text or empty string if none
    """
    try:
        url = f"https://registrationssb.ucr.edu/StudentRegistrationSsb/ssb/searchResults/getSectionPrerequisites?term={term}&courseReferenceNumber={course_reference_number}"
        response = session.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check if no prerequisites
        if "No prerequisite information available" in response.text:
            return ""
        
        # Extract prerequisite text from the structured HTML
        prereq_section = soup.find('section', {'aria-labelledby': 'preReqs'})
        if not prereq_section:
            return ""
        
        # Get all <pre> tags which contain the prerequisite text
        pre_tags = prereq_section.find_all('pre')
        if not pre_tags:
            return ""
        
        # Combine all prerequisite text
        prerequisite_text = ''.join(tag.get_text().strip() for tag in pre_tags)
        return prerequisite_text.strip() if prerequisite_text else ""
        
    except Exception as e:
        print(f"Error fetching prerequisites for CRN {course_reference_number}: {e}")
        return ""


def fetch_course_data(term: str = "202440", include_prerequisites: bool = True) -> list[dict]:
    """
    Fetch course data from the UCR registration system.
    
    Args:
        term: Term code (e.g., "202440" for Fall 2024)
              Format: YYYY + QQ where QQ is 10=winter, 20=spring, 30=summer, 40=fall
        include_prerequisites: Whether to fetch prerequisite information for each course
    
    Returns:
        list[dict]: A list of dictionaries, each containing course data.
    """
    
    # get session cookies
    session = requests.Session()
    session.get("https://registrationssb.ucr.edu")
    
    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    
    # initialize search session
    session.post(
        "https://registrationssb.ucr.edu/StudentRegistrationSsb/ssb/term/search?mode=search",
        data={"term": term},
        headers=headers,
    )
    
    # get total count first
    url = f"https://registrationssb.ucr.edu/StudentRegistrationSsb/ssb/searchResults/searchResults?txt_term={term}&pageOffset=0&pageMaxSize=1&sortColumn=subjectDescription&sortDirection=asc"
    response = session.get(url, headers=headers)
    response.raise_for_status()
    
    total_count = response.json()["totalCount"]
    print(f"Total courses available: {total_count}")
    
    # fetch all courses with pagination
    courses = []
    page_offset = 0
    page_size = 500  # reasonable batch size
    
    while page_offset < total_count:
        print(f"Fetching courses {page_offset} to {min(page_offset + page_size, total_count)}...")
        
        url = f"https://registrationssb.ucr.edu/StudentRegistrationSsb/ssb/searchResults/searchResults?txt_term={term}&pageOffset={page_offset}&pageMaxSize={page_size}&sortColumn=subjectDescription&sortDirection=asc"
        response = session.get(url, headers=headers)
        response.raise_for_status()
        
        batch_data = response.json()["data"]
        if not batch_data:
            print("No more data returned, stopping...")
            break
            
        courses.extend(batch_data)
        page_offset += len(batch_data)
        
        # break if we got less than expected (last page)
        if len(batch_data) < page_size:
            break
    
    print(f"Successfully fetched {len(courses)} courses")
    
    # Fetch prerequisites if requested
    if include_prerequisites:
        print("Fetching prerequisites for all courses...")
        for i, course in enumerate(courses):
            if (i + 1) % 100 == 0:
                print(f"Fetched prerequisites for {i + 1}/{len(courses)} courses...")
            
            crn = course.get("courseReferenceNumber")
            if crn:
                prerequisites = fetch_prerequisites(session, term, crn)
                course["prerequisites"] = prerequisites
    
    return courses


def makeCSV(term: str, courses: list[dict], fieldNames: list[str]):
    #create appropriate filename to store data into
    fileName = f'./{term}.csv'
    try:
        with open(fileName, 'w', newline='') as csvfile:
            # Create a DictWriter object
            writer = csv.DictWriter(csvfile, fieldnames=fieldNames)

            # Write the header row
            writer.writeheader()

            # Write the data rows
            writer.writerows(courses)

        print(f"Data successfully written to {fileName}")

    except IOError as e:
        print(f"I/O error: {e}")



def looper():
    earliestYear = 2018

    #go through each of the quarters from winter of the earliest year to fall of 2024 and to retrieve data
    for year in range(earliestYear, 2025):
        for quarter in [10, 20, 30, 40]:
            #set appropriate inputs
            term = str(year*100 + quarter)
            includePrerequisites = False
            #fetch course data
            courses = fetch_course_data(term, includePrerequisites)
            #write couse data into CSV
            fieldNames = list(courses[0].keys())
            makeCSV(term, courses, fieldNames)


if __name__ == '__main__':
    looper()