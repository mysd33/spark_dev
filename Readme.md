#AP基盤プロトタイプについて

[![Build Status](https://dev.azure.com/Masashi11Yoshida/scala_dev/_apis/build/status/databricks_dev?branchName=master)](https://dev.azure.com/Masashi11Yoshida/scala_dev/_build/latest?definitionId=1&branchName=master)

![Build Status](https://codebuild.ap-northeast-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoidWczL1JYaDlsdVI3alFTNk1QYTBVc3ZkcytXU05NWDUxMndLZ2k5TkdEdVQrZUdLREpWYnpqWkw0Mm5NSmhRWGJsekcxbGlpVkdHNjY0Z0NLdjdwdVFnPSIsIml2UGFyYW1ldGVyU3BlYyI6ImVMUlhpZFNKenk3KzZNQ0MiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master)

##プロジェクト構成
* databricks_dev ルートプロジェクト
  * application 業務APサブプロジェクト
  * dbconnectApplication Databricks接続（DatabricksConnect）用業務APサブプロジェクト
  * sparkFramework SparkAPのAP基盤サブプロジェクト。Spark標準機能のみに依存
  * sparkTestFramework SparkAP用の拡張テストフレームワークサブプロジェクト
  * databricksFramework Databricks固有の機能に依存するAP基盤サブプロジェクト
  * notebooks Databricks Notebookと同期されたNotebookソースコードフォルダ
  * testdata 業務APを動作させるためのテストファイル

##IntelliJからのAPの端末ローカルSpark実行
* 通常、APは「application」プロジェクトに作成します
* 端末ローカルでSpark実行します
* この起動方法は、Databricks依存機能は実行できません
* 別途ローカル実行用の環境セットアップが必要(Teams Wiki参照)です
* サンプルを動作させる際は、testdata/inputディレクトリにあるデータを「C:\temp」にコピーしてください
  * applicaitonプロジェクト/src/main/resources/application-dev.propertiesのbasepathプロパティを変更すれば違うディレクトリにも変更可能です
* AP起動方法
  * 「構成の編集」で「アプリケーション」を作成
  * 「メインクラス」に「com.example.fw.app.ApplicationEntryPoint」を設定
  * 「プログラムの引数」に対象Logicクラスの完全修飾名を設定
    * 例）com.example.sample.logic.SampleDataSetBLogic3
  * 「VMパラメータ」に「-Dactive.profile=dev」を設定
    * または「環境変数」に「ACTIVE_PROFILE=dev」と設定
  * 「作業ディレクトリ」はプロジェクトのルートフォルダを設定
    * 例）C:\Users\xxxx\IdeaProjects\databricks_dev
  * 「クラスパスとJDK」は「application」を設定

##IntelliJからのAPのDatabricks接続実行
* 端末からAzure上のDatabricksクラスタに接続しSpark実行します
* Databricks依存機能も実行できます
* 別途Databrics接続実行用の環境セットアップが必要(Teams Wiki参照)です
  * 「構成の編集」で「アプリケーション」を作成
  * 「メインクラス」に「com.example.app.EntryPoint」（dbconnectApplication上のクラス）を設定
  * 「プログラムの引数」に対象Logicクラスの完全修飾名を設定
    * 例）com.example.sample.logic.SampleDataSetBLogic3
  * 「VMパラメータ」に「-Dactive.profile=dbconnect」を設定
    * または「環境変数」に「ACTIVE_PROFILE=dbconnect」と設定
  * 「作業ディレクトリ」はプロジェクトのルートフォルダを設定
    * 例）C:\Users\xxxx\IdeaProjects\databricks_dev
  * 「クラスパスとJDK」は「dbconnectApplication」を設定
  
##ビルド
* IntelliJのsbt shellの場合
    ```
    > package
    ```
* sbtコマンドの場合
  ```
  sbt package
  ```

##単体テスト
* 単体テストコードは、「application」プロジェクトの「src/test/scala」ディレクトリに格納します
* IntelliJで指定したテストクラス実行の場合
  * 「構成の編集」で「ScalaTest」を作成
  * 「テストクラス」に対象テストクラスを設定
  * 「VMパラメータ」に「-Dactive.profile=ut」を設定
    * または「環境変数」に「ACTIVE_PROFILE=ut」と設定
  * 「作業ディレクトリ」はプロジェクトのルートフォルダ
  * 「クラスパスとJDK」は「application」

* IntelliJのsbt shellの場合  
  * 「ファイル」-「設定」で、「ビルド、実行、デプロイ」の「sbt」の設定で「VMパラメータ」で「-Dactive.profile=ut」を設定しておく
  * 以下実行
  ```
  > test
  ```
* sbtコマンドの場合
  ```
  sbt -Dactive.profile=ut test
  ```
  
##実行可能jar（アセンブリ）作成 
* Databricks/Sparkクラスタ上でAPを実行するためには「sbt package」コマンドで生成するjarではなく、「sbt assembly」を使って必要なclassファイル等を全て1つにまとめた実行可能jarを作成します
* なお、テストをスキップするように設定済です
* 実行すると「（ルートディレクトリ）/target/scala-2.11/」フォルダに「databricks_dev-assembly-0.1.jar」が作成されます
* IntelliJのsbt shellの場合  
  ```
  > assembly
  ```
* sbtコマンドの場合
  ```
  sbt assembly
  ```

## Databricksでのライブラリ（jar）登録
* 実行可能jar「databricks_dev-assembly-0.1.jar」を下記ドキュメントの通り、ワースペースライブラリとして登録し、クラスタに同jarをインストールします
  * https://docs.microsoft.com/ja-jp/azure/databricks/libraries

## Databricksでのライブラリを用いたnotebook実行
* 上記手順でjarをクラスタにライブラリインストールすると、Notebook上で、jarに含まれるクラスを呼び出すことができます
* 以下のNotebookのscalaサンプルコードが参考になります。
  * databricks_dev\notebooks\Users\admin@mysd33.work\testFramework.scala

## Databricksでのjarジョブ実行手順
* 下記ドキュメントの通り、ジョブを作成し、jarをアップロードして実行するか、ワークスペース上のjarを指定します
  * https://docs.microsoft.com/ja-jp/azure/databricks/jobs#create-a-job

## DatabricksでのNotebookジョブ実行手順
* 下記ドキュメントの通り、ジョブを作成し、notebookを指定します
  * https://docs.microsoft.com/ja-jp/azure/databricks/jobs#create-a-job

## Azure DataFactoryからのDatabricksの実行
* 同様にDataFactory Databricks jarアクティビティやNotebookアクティビティを実行できます。
  * https://docs.microsoft.com/ja-jp/azure/data-factory/transform-data-databricks-jar
  * https://docs.microsoft.com/ja-jp/azure/data-factory/transform-data-databricks-notebook
* サンプルソースとして、別のリポジトリ（adf_dev）で管理されているので、そちらを参考にしてください。

## Azure Synapse Analyticsとの連携
* SparkのAPIを使って、Databricksから（Blobストレージを経由し）直接Synapse Analyticsのテーブルへの書き込みが可能です。
  * （参考）
    * https://docs.microsoft.com/ja-jp/azure/databricks/scenarios/databricks-extract-load-sql-data-warehouse#create-an-azure-databricks-service
    * https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/azure/synapse-analytics
* 接続準備として以下を実施します
  * Azure Synapseで、SQL Pool（DWH）を作成
    *（参考）https://docs.microsoft.com/ja-jp/azure/synapse-analytics/sql-data-warehouse/create-data-warehouse-portal
  *　作成したデータベースに対して、Azure Synapseに使用するマスターキーを作成
    * （参考）https://docs.microsoft.com/ja-jp/sql/relational-databases/security/encryption/create-a-database-master-key?view=sql-server-ver15
  * 一時ストレージ用のAzure BLobストレージを作成（Databricksでの処理用に作成済のものでもOK）
* Synapseの接続情報とBlobストレージのアカウント情報をDatabricksのシークレットに格納
  * （参考）https://docs.microsoft.com/ja-jp/azure/databricks/security/secrets/secrets
  * Databricksの独自シークレットでも、プレビューのKeyVaultを使った方法のどちらでもよい
* 接続情報をapplication-prod.propertiesに設定
```
sqldw.url.scope=（SynapseAnalyticsのURLをDatabricksのシークレット登録したときのスコープ名）
sqldw.url.key=（SynapseAnalyticsのURLをDatabricksのシークレット登録したときのシークレットキー名）
sqldw.blob.tempdir.url=（一時ストレージ用のBlobストレージのURL）
sqldw.blob.accountkey.name=（一時ストレージ用のBlobストレージのアカウントキー名）
sqldw.blob.accountkey.scope=（一時ストレージ用のBLobストレージのアカウントキーをシークレット登録したときのスコープ名）
sqldw.blob.accountkey.key=（一時ストレージ用のBLobストレージのアカウントキーをシークレット登録したときのシークレットキー名）
```
## Azure Monitorとの連携
* 以下のサイトで紹介されているライブラリをつかうことで、アプリケーションログとメトリックを、AzureMonitorログ（LogAnalytics）へのログ転送できます。
  * （参考）
    * https://github.com/mspnp/spark-monitoring/blob/master/README.md
* 上記サイトで記載の通り、jarをビルドします。src/targetフォルダにjarが作成されます
  * spark-listeners_2.4.5_2.11-1.0.0.jar
  * spark-listeners-loganalytics_2.4.5_2.11-1.0.0.jar
* LogAnalyticsワークスペースを作成しID、KEYを取得し/src/spark-listeners/scripts/spark-monitoring.shの以下を編集します
　（参考サイトのとおり、プレビュー機能のKeyVaultを使った場合は別の編集方法になります）
```
export LOG_ANALYTICS_WORKSPACE_ID= <LogAnaltyics Workspace ID>
export LOG_ANALYTICS_WORKSPACE_KEY= <Log Analytics Workspace キー>
```
* Databricks CLIで以下を実行し、spark-monitoring.shとjarファイルをDBFS上にコピーします。
```
dbfs mkdirs dbfs:/databricks/spark-monitoring
dbfs cp <local path to spark-monitoring.sh> dbfs:/databricks/spark-monitoring/spark-monitoring.sh
dbfs cp --overwrite --recursive <local path to target jars folder> dbfs:/databricks/spark-monitoring/
```
* コンソールより、Databricksクラスタを起動します。この時、「Init Scripts」タブで、Init Scriptsとして、「dbfs:/databricks/spark-monitoring/spark-monitoring.sh」を追加します。

* 当該APのapplication/src/main/resources/application-prod.propertiesの設定を変更します。
  * log4j.overwrite=trueにすると、application/src/main/resources/com/example/log4j-prod.propertiesによるlog4jの設定が有効になります
```
log4j.overwrite=true
```
* ビルドし当該APのアセンブリjar（databricks_dev-assembly-0.1.jar）をDatabricksクラスタへデプロイし、実行します。

* 作成したLogAnalyticsへログが転送されるようになります。LogAnalyticsのログで、例えば、以下のクエリを実行すると、アプリケーション実行時のログが検索できます
```
SparkLoggingEvent_CL
| where logger_name_s contains "com.example"
```
 
## AWS EMRでのステップ実行手順
* エントリポイントにApplicationEntryPointクラスを使用することで、AP基盤のDI機能によりSpark標準機能のみを使用したSparkAPとしてAWS EMR上で実行できます。
* S3のバケットにフォルダを作成しデータを配備（例：s3://xxxxbucket/mystorage/）  
* EMRで動作するようアプリケーションの設定を変更します
  * application.propertiesをactive.profile=prodawsに変更
  * application-prdaws.propertiesのbasepathをデータを配備するS3のURLを指定（例：s3://mysd33bucket123/mystorage/）
* sbt assemblyコマンドで実行可能jarを作成します(databricks_dev-assembly-0.1.jar)
* 作成したjarをS3にを配備（例：s3://xxxxbucket/app/databricks_dev-assembly-0.1.jar）
* EMRでクラスタを作成
  * 起動モードは「ステップ実行」を選択
  * ステップタイプに「Sparkアプリケーション」を選択し、設定をクリック。以下の通り設定
    * Spark-sumitオプション
      * --class com.example.fw.app.ApplicationEntryPoint
    * アプリケーションの場所
      * アプリケーションを配備したS3のURL
      * 例：s3://xxxxbucket/app/databricks_dev-assembly-0.1.jar
    * 引数
      * LogicクラスのFQDNを指定
      * 例：com.example.sample.logic.SampleDataSetBLogic3
  * ソフトウェア設定、ハードウェア構成を適切に設定し、「クラスタを作成」をクリック

## AWS DataPipelineによるSparkジョブ実行
* 同様に、AWS DataPipelineのEmrActivityを使ってEMRを起動しSparkジョブを実行できます。
  * 「Create Pipeline」をクリック
  * 「Build using a template」で「Run jbo on an Elastic MapReaduce cluster」を選択
  * 「EMR step(s)」に以下を記入
    ```
     command-runner.jar,spark-submit,--deploy-mode,cluster,--class,com.example.fw.app.ApplicationEntryPoint,s3://(バケット名)/（フォルダ名）/databricks_dev-assembly-0.1.jar,com.example.sample.logic.SampleDataSetBLogic3
    ```
  * 「Core node instance type」に「m5.xlarge」
  * 「EMR Release Label」に「emr-5.30.1」
  * 「Core node instance count 2」
  * 「Master node instace type」に「m5.xlarge」
  * 「Schedule」を指定
    * お試しで実行するなら「on pipeline activation」を選択しておくとよい
  * 「S3location for logs」で、ログを格納するS3のパスを指定
  * 「Edit Architect」を選択
  *  EMR Cluster Objectの編集画面で「Add an option field」で「Applications」を追加し「spark」を記述
    * デフォルトではEMRクラスタに、HiveとPigしかインストールされないため、sparkをインストールするようにする
  * 「Activate」を実行する
* すでにjson設定化したファイルが以下のフォルダにあるので、それを使ってCLI実行しても良い
  * aws-datapipelineフォルダ配下のawsdatapipeline.json
  * jsonファイルのマネージメントコンソールでのインポートがうまくいかないので注意
    
    * パイプラインを新規作成
    ```
    aws datapipeline create-pipeline --name tutorialEMRCLI --unique-id tutorialEMRCLI-token
    ```
    * 下のようなパイプラインIDが返却される
    ```
    {
        "pipelineId": "df-03617342CEQKZU3JZOXQ"
    }
    ```
    * パイプライン定義(json）をアップロード
    ```
    aws datapipeline put-pipeline-definition --pipeline-id df-03617342CEQKZU3JZOXQ --pipeline-definition file://awsdatapipeline.json
    ```
    * 定義がアップロードされたことを確認
    ```
    aws datapipeline get-pipeline-definition --pipeline-id df-03617342CEQKZU3JZOXQ
    ```
    * パイプラインをアクティベート。パイプラインが実行されEMRのジョブが起動する
    ```
    aws datapipeline activate-pipeline --pipeline-id df-03617342CEQKZU3JZOXQ
    ```
    
##Azure DevOps PipelineでのCI
* Azure Reposでソースコード管理し、Azure Pipelineでパイプラインを作成することでazure-pipelines.ymlの定義に基づきPipeline実行できます
* ビルド、scaladoc、単体テスト実行、結合テスト実行、実行可能jar作成、テスト結果レポート、カバレッジレポート、SonarQubeによる静的コード解析レポートに対応しています
* また、ivyローカルリポジトリのjarをキャッシュしビルド時間を短縮する設定もしています
* 本サンプルではAzure DevOps Serviceを使って動作確認しています
  * 本番開発では、東日本リージョン内でのDevOps Serverの構築が必要です

##Azure DevOps PipelineからのSonarQubeの実行
* Azure DevOps Pipelineで、SonarQubeを使用した静的コードチェックを実施しています
* 利用しない場合は、azure-pipeline.ymlの該当箇所をコメントアウトしてください
  * 「task: SonarQubePrepare@4」、「task: SonarQubePrepare@4」、「task: SonarQubePublish@4」
* Azure上でのSonarQubeの簡単な構築方法は、以下の通りです
  * 本番ではそのままの手順で構築しないこと
  * （参考ページ）https://azuredevopslabs.com/labs/vstsextend/sonarqube/

* Azure Container InstancesでSonarQubeのコンテナを起動します
```
az container create -g (リソースグループ名)  --name sonarqubeaci --image sonarqube --ports 9000 --dns-name-label （DNSラベル名）--cpu 2 --memory 3.5
（例）
az container create -g RG_MYSD_DEV  --name sonarqubeaci --image sonarqube --ports 9000 --dns-name-label myd33sonarqube --cpu 2 --memory 3.5
```
* SonarQubeのコンテナのFQDNを調べて、ブラウザ起動します
  * 「http://（az containerコマンドの--dns-name-labelオプションの値）.（リージョン）.azurecontainer.io:9000」になります  
  * （例）http://myd33sonarqube.japaneast.azurecontainer.io:9000

* SonarQubeにログインしプロジェクトを作成します
  * admin/adminでログイン（ログイン後、パスワード変更しておくこと）
  * プロジェクトを作成
    * name: databricks_dev
    * projectkey: databricks_dev
    * visibility: private
  * プロジェクトのページに移動し、トークンを生成し、端末にコピーし保管します
  * nameやprojectkeyはsonar-project.propertiesの値と合わせていますので変更する場合は、sonar-project.propertiesの値も修正してください
    
* Azure PipelineのExtensionとしてSonarQubeをインストール
  * Azure DevOpsのOrganization Sttings->Exntensionsで「Browse MarketPlace」から「SonarQube」を取得
    * Azure DevOps Organizaiont管理者へのExtensionインストールの承認フローが入りインストールを完了させます

* SonarQube Serverの接続情報を登録します 
  * Project Setting -> Pipelines -> Service connectionsへ移動
  * Create Service Connectionをクリック
  * 「SonarQube Server」を選択し、「Server Url」と「Token」「Service connection name」を入力します
  * 「Service connection name」はazure-pipelines.yamlの「task: SonarQubePrepare@4」の「SonarQube」の設定と合わせて'SonarQube'とします。
    *  'SonarQube'以外の名前にしたい場合は、azure-pipeline.yamlの値を直してください

## Azure DevOpsからのCD
 * 参考
   * https://docs.microsoft.com/ja-jp/azure/databricks/dev-tools/ci-cd/ci-cd-azure-devops#define-your-release-pipeline
 * 初めて使用する場合、MarketPlaceから「Databricks Script Deployment Task by Data Thirst」のExtentionを追加
   * https://marketplace.visualstudio.com/items?itemName=riserrad.azdo-databricks&targetId=97d4df0c-2f78-42f0-844c-1ba9d56e3f8a
   * Azure DevOpsのOrganization Sttings->Exntensionsで、Azure DevOps Organizaiont管理者へのExtensionインストールの承認フローが入りインストールを完了させます
 * PipelinesからReleaseパイプラインを作成   
 * 「Artifacts」で「Add」をクリックし、ビルドパイプラインで出力したアーティファクトの設定
   * Project、Source（ビルドパイプラインのリポジトリ）、Default Version、Source aliasを設定
 * 「Stages」で「Add」をクリックし、「Empty Job」（空のジョブ）を追加 
 * Add Taskでタスクを追加し「Azure Key Vault」を追加
   * Azure Subscriptionを選択   
   * Key Vaultを選択
     * 事前にKey Vaultを作成しシークレットにDatabricksのパーソナルアクセストークンを登録しておく
   * 以降のタスクで、$(シークレット名）の形式で、変数として利用できる。
 * Add Taskでタスクを追加し「Use Python version」を追加
   * Version specは3.x   
 * Add Taskでタスクを追加し「Databricks DBFS File Deployment」を追加
   * jarをDBFS上にインストール
   * Azure Resionは、「japaneast」
   * Local Root Folderは、「$(System.DefaultWorkingDirectory)/_databricks_dev/applicationAssembly/target/scala-2.11/」
   * Target folder in DBFSは、「/FileStore/jars/」
   * Bearer Tokenを「$(KeyVaultのシークレット名)」設定
     * 事前に設定したシークレットから取得するようにする
 * Add Taskでタスクを追加し「Command Line Script」を追加
   * pip install requests 
 * Add Taskでタスクを追加し「Python script」を追加   
   * cd-scripts/installLibrary.pyをつかってDatabricks上にjarインストール
   * Script Pathは「$(System.DefaultWorkingDirectory)/_databricks_dev/applicationAssembly/cd-scripts/installLibrary.py」
     * Databricks REST API（Library API)を使用しています。
     * REST APIの仕様上、クラスタが終了状態（Terminated）か存在しない場合にはインストールできないためパイプラインはエラーになります
     * https://docs.microsoft.com/ja-jp/azure/databricks/dev-tools/api/latest/libraries
   * Argumentsに以下を指定
     * --shard=XXX --token=$(KeyVaultのシークレット名) --clusterid=XXX --libs=XXX --dbfspath=XXX
       * XXXのところの各パラメータ値は事前に変数定義して利用すると汎化できる
     * shard - ワークスペースのURL
       * 例：https://xxxx.azuredatabricks.net
       * 最後にスラッシュを入れないこと
     * token - ワークスペースのアクセストークン（personal access token）
     * clusterid - クラスタID
     * libs - jarを含んでいた元のライブラリフォルダ
       * $(System.DefaultWorkingDirectory)/_databricks_dev/applicationAssembly/target/scala-2.11/
     * dbfspath - DFS上のライブラリのパス
       * /FileStore/jars
       * 最後にスラッシュを入れないこと

## AWS CodePipeline AWS CodeBuildでのCICD
 * AWS CodeCommitでソースコード管理し、AWS CodeBuildでbuildspec.ymlの定義に基づきCI実行できます
 * AWS CodePipelineからBuild時にCodeBuildを呼び出すように設定することで、ソース変更を自動検知しCICD自動実行できます。
 * ビルド、scaladoc、単体テスト実行、結合テスト実行、実行可能jar作成、テスト結果レポートやカバレッジレポートに対応しています
 * ivyローカルリポジトリのjarをキャッシュしビルド時間を短縮する設定もしています
 * CodeBuildからのSonarQubeの実行にも対応しています。
    * SonarQubeサーバの構築手順は省略します
    * SonarQubeのプロジェクト名(name)、プロジェクトキー(project_key)を"spark_dev"にしてください。
      * buildspec.yamlのvariablesに「SONAR_PROJECTNAME: "spark_dev"」で設定しています
    * SystemManager ParameterStoreに、以下２つのパラメータ値を設定して下さい。
      * /CodeBuild/SonarQubeEndpoint →　SonarQubeのエンドポイントURL（Textで設定）
      * /CodeBuild/SonarQubeToken → トークンの値（SecureStringで設定）
    * CodeBuildのIAMロールにSytemManager ParameterStoreとKMSのキーにアクセスするポリシーを割り当ててください。
      * (参考)https://dev.classmethod.jp/articles/codebuild-env/