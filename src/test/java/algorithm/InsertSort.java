package algorithm;

/**
 * @author wangguochao
 * @date 2022/9/16
 */
public class InsertSort {

    public void insertionSort(int[] a, int n) {
        if (n < 1) return;

        for (int i = 0; i < n; ++i) {
            int value = a[i];
            int j = i - 1;
            // 查找插入的位置
            for (; j >= 0; --j) {
                if (a[j] > value) {
                    a[j+1] = a[j]; // 数据移动
                } else {
                    break;
                }
            }
            a[j+1] = value; // 插入数据
        }
    }
}
