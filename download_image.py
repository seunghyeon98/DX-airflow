
import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG

# BashOperater는 리눅스 코드를 작동시키는 것이고
from airflow.operators.bash import BashOperator
# PythonOperator는 파이썬 코드를 작동 시키는 것이다.
from airflow.operators.python import PythonOperator

#인스턴스 생성 - 모든 워크플로의 시작점
#dag 이름 과 설명 그리고 시작 날짜 및 실행 간격 설정
dag = DAG(
    dag_id="download_image",
    description="Download Image",

    # start_date 는 실행시작 날짜이다.
    start_date=airflow.utils.dates.days_ago(14),

    # 주기를 표시하는 설정이다. None은 주기가 없다
    schedule_interval=None
        # 현재는 주기가 없어서 직접 트리거를 발생시켜야 한다.

)


# 위에서 파이썬 코드를 작성했다면
# 이제, 리눅스 작업을 생성하자.

# 리눅스 작업을 생성
# 웹서버 api데이터는 되도록이면, 리눅스 명령어인 curl명령어를 이용하는 것이 편리하다.
download_launches = BashOperator(
    task_id ='download_launches',
    bash_command = "curl -o /tmp/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming/",
    dag =dag
)


# 웹에서 가져온 데이터를 파싱하고, 이미지 경로에서 이미지를 다운로드 받아서 저장
def _get_pictures():
    # 이미지를 저장할 디렉토리를 생성
        # 이 명령어는 tmp 경로에 images폴더가 없으면 images폴더를 만들어주는 명령어이다.
    pathlib.Path("/tmp/images").mkdir(parents=True,exist_ok=True)

    # json 데이터를 pyhton 으로 파싱하고, 이미지 경로를 추출
    with open("/tmp/launches.json") as f:
        launches = json.laod(f)
        # results 키의 값을 가져와, 한 개씩 순회하며 image키의 값을 
        # 모아서 리스트를 생성한다.

        image_urls = [ launch['image'] for launch in launches['results']]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)

                # 파일의 이름을 뽑을 때는, '/'문자를 기준으로 나누고,
                # 리스트의 마지막 원소가, 파일의 이름이 된다.
                image_filename = image_url.split('/')[-1]
                target_file = f"/tmp/images/{image_filename}"

                # 파싱한 이미지를 "/tmp/images경로에 각 이름대로 저장한다."
                with open(target_file,"wb") as f:
                    f.write(response.content)
            
            # 예외처리
                # -> 배열을 순회하면서, 파일을 저장할 때 예외처리를 해주자.
            except requests_exceptions.MissingSchema: 
                print(f"{image_url} 은 잘못된 url이다.")



# 이제 이미지를 불러오는 파이썬 함수 코드를 파이썬 operator를 통해 작성해보자!
get_images = PythonOperator(
    task_id = "get_pictures",
    python_callable=_get_pictures,
    dag = dag
)

# 알림설정을 위한 Operator
notify = BashOperator(
    task_id = 'notify',
    bash_command= 'echo "이미지 저장성공~"',
    dag = dag
)


# 태스크 실행 순서 설정
# 각각 정의해둔 operator들의 순서를 정의한다.
download_launches >> get_images >> notify