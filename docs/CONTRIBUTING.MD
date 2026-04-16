# MetroPulse

## Branching Strategy

1. **`main`**: Chỉ chứa code đã chạy ổn định 100%. **Tuyệt đối không commit thẳng.**
2. **`dev`**: Nhánh tích hợp. Mọi người gộp code vào đây để test độ tương thích.
3. **`feature/<ten-chuc-nang>`**: Nhánh làm nhiệm vụ mới. 
   - Ví dụ: `feature/spark-cleaning`, `feature/kafka-producer`.
4. **`fix/<ten-loi>`**: Nhánh sửa lỗi khẩn cấp.
   - Ví dụ: `fix/api-connection-timeout`.

---

## Commit Message Convention

Chúng ta sử dụng chuẩn **Conventional Commits**. Cú pháp:
`type(scope): description`

* **feat**: Thêm tính năng mới.
* **fix**: Sửa lỗi.
* **docs**: Cập nhật tài liệu (README, CONTRIBUTING).
* **refactor**: Tối ưu code nhưng không thay đổi chức năng.
* **chore**: Các việc vặt (cài thư viện, update .gitignore).

*Ví dụ:* `feat(ingestion): add weather api producer script`

---

## Pull Request Process (PR)

Khi hoàn thành task trên nhánh cá nhân, hãy thực hiện các bước:
1.  Kéo code mới nhất từ `dev` về nhánh của mình và xử lý conflict (nếu có).
2.  Push nhánh lên GitHub.
3.  Tạo Pull Request gộp vào `dev`.
4.  **Bắt buộc:** Phải có ít nhất 1 thành viên khác vào Review và bấm **Approve** thì mới được gộp (Merge).
5. **Môi trường:** Luôn cập nhật file `requirements.txt` hoặc `docker-compose.yml` nếu bạn thêm thư viện/dịch vụ mới.
