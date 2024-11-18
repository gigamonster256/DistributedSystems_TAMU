java_files=$(find . -depth -maxdepth 1 -name "*.java")
echo "java files: $java_files"

output_dirs=$(find . -depth -maxdepth 1 -name "output*")
echo "output dirs: $output_dirs"

png_files=$(find . -depth -maxdepth 1 -name "*.png")
echo "png files: $png_files"

zip -r submission.zip $java_files $output_dirs $png_files