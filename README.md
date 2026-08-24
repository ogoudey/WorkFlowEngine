# Wave 1 Blocks
```mermaid
graph LR
    A([FinetuneGroot]) --- B(IsaacSimContainer)--- C[heavy p.large]
    D([HandRemoval]) --- E(HandRemovalContainer) --- C
    F([LeRobotConversion]) --- G(LeRobotConversionContainer) --- H[medium unsure]
    I([VideoEdit]) --- J(VideoEditContainer) --- H
    X([TestBlockCommand]) --- Y(TestContainer) --- Z[minimal]
    X1([TestAWSCommand]) --- Y
    X2([SyncS3BucketCommand]) --- Y
    X3([TestLongBlockCommand]) --- Y

%% Define Color Schemes for Statuses
classDef environmentCreated fill:#2563eb,stroke:#1d4ed8,color:#fff
classDef pushedToECR fill:#0d9488,stroke:#0f766e,color:#fff
classDef cloudBatch fill:#d97706,stroke:#b45309,color:#fff
classDef cloudTest fill:#16a34a,stroke:#15803d,color:#fff

class C environmentCreated
class H environmentCreated
class Z environmentCreated
class Y pushedToECR

class X cloudBatch
class X1 cloudBatch
class X2 cloudBatch
```

Local test doesn't mean much. I don't test these containers locally. I might test some of its software locally.

### This is how you build a container and push to AWS ECR.
```bash

aws ecr get-login-password --region us-east-2 | sudo docker login --username AWS --password-stdin 538091937392.dkr.ecr.us-east-2.amazonaws.com

REPO="blocks/<block_name>"
cd container
docker build -t $REPO . --no-cache
docker tag $REPO:latest 538091937392.dkr.ecr.us-east-2.amazonaws.com/$REPO:latest
docker push 538091937392.dkr.ecr.us-east-2.amazonaws.com/$REPO:latest
# To Test
sudo docker run --rm REPO:latest <command>
```