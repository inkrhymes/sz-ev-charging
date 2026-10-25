# CI workflow (staged)

`ci.yml` 需放到 `.github/workflows/ci.yml` 才会生效。当前推送凭据缺少 GitHub `workflow` 权限，无法直接推送 workflow 文件；有权限的人执行：

```bash
mkdir -p .github/workflows && git mv ci/ci.yml .github/workflows/ci.yml && git commit -m "ci: enable GitHub Actions" && git push
```
