# Simple MapReduce Assignment (Student Guide)

本作业分为两层：

- 第 1 层（框架核心）：理解并完成 MapReduce 的核心逻辑片段
- 第 2 层（任务实现）：基于框架完成 4 个任务

---

## 你需要改哪些文件

只需要修改这两个文件中带有 TODO 标记的代码块：

- `mr_core.py`
  - `################ STUDENT TODO (Layer 1 core) BEGIN ################`
  - 你只需要完成 `shuffle_stage` 的核心逻辑（按 key 分组）
- `jobs.py`
  - `################ STUDENT TODO (Layer 2) BEGIN ################`
  - 需要完成 tokenizer、各任务的 mapper/reducer 等核心函数

当前模板中的 TODO 区块是占位逻辑（会给 warning 并返回默认值），你必须自行修改并实现这些函数，否则结果不会正确。

请不要修改以下文件：

- `run_job.py`（固定运行入口）（不要改动，会影响教师端自动评测分数）
- `run_job.sha256`（入口完整性校验）
- `generate_student_subset.py`（数据子集生成脚本，已提供）

---

## 四个任务说明

1. **Word Count**
   - mapper: 输出 `(token, 1)`
   - reducer: 对同一 token 求和

2. **Inverted Index**
   - mapper: 输出 `(token, doc_id)`，同一文档内 token 去重
   - reducer: 合并为去重且排序的 doc_id 列表

3. **Prefix Filter**
   - mapper: 仅输出以 prefix 开头的 token 对应 `(token, 1)`
   - reducer: 统计匹配 token 频次

4. **Similarity (Jaccard)**
   - mapper: 对共享同一 token 的文档两两配对，输出 `(doc_i||doc_j, 1)`
   - reducer: 累加得到交集大小（intersection count）
   - 后处理会在框架里计算 Jaccard 并做阈值过滤

---

## 如何运行

运行前请先完成项目中所有 `TODO` 标记的代码块；未完成时会出现 warning。

第一步：先生成你的个人数据子集（示例学号目录为 `学号`）：

```bash
python generate_student_subset.py \
  --student-id 学号 \
  --input master_arxiv_cs_ai_2026_to_2026_03_12.jsonl \
  --sample-size 1000 \
  --output-dir student_release
```

# 示例，学号20220001：
python3 generate_student_subset.py \
  --student-id 23076071 \
  --input \ master_arxiv_cs_ai_2026_to_2026_03_12.jsonl \
  --sample-size 1000 \
  --output-dir student_release

第二步：完成作业

完成所有的 todo

第三步：运行作业：

```bash
python run_job.py \
  --student-dir "student_release/学号" \
  --task all \
  --prefix trans \
  --output "student_release/学号/output/report.json"
```

如果运行成功，会生成：

- `student_release/学号/papers.jsonl`
- `student_release/学号/config.json`
- `student_release/学号/assigned_meta.json`
- `student_release/学号/output/report.json`

---

## 如何查看中间结果（调试用）

可以运行下面脚本查看每个任务的 map / shuffle / reduce 中间输出预览：

```bash
python3 inspect_intermediate.py \
  --student-dir "student_release/23076071" \
  --task all \
  --prefix trans \
  --limit 5 \
  --record-limit 30
```

常用参数：

- `--task`: `all | word_count | inverted_index | prefix_filter | similarity`
- `--limit`: 每个阶段最多显示多少条预览
- `--record-limit`: 只取前 N 条记录做快速测试（推荐先小样本调试）
- `--num-workers`: 可覆盖 `config.json` 里的 worker 数

如果你只想看一个任务，例如词频统计：

```bash
python inspect_intermediate.py \
  --student-dir "student_release/学号" \
  --task word_count \
  --limit 10 \
  --record-limit 20
```

---

## 你需要提交什么

请提交以下文件（或包含它们的压缩包）：

- `student_release/<你的学号>/output/report.json`

会根据 `report.json` 进行评估项目完成情况，请大家在各自设备上完成，注意代码会查重，所以不要开源你的代码。

---

## 注意事项

- 保持函数签名不变（不要改参数名和返回格式）
- 结果要可重复（同样输入多次运行结果一致）
- 优先保证正确性，再考虑代码简洁性
- 如非明确要求，不要新增第三方依赖
